import asyncio
import json
import re
from datetime import datetime, timedelta, timezone
from functools import lru_cache

import aiohttp
import pytz
import requests
from async_lru import alru_cache
from loguru import logger
from tqdm import tqdm

from openf1.services.ingestor_livetiming.core.decoding import decode
from openf1.services.ingestor_livetiming.core.objects import (
    Document,
    Message,
    get_collections,
    get_source_topics,
)
from openf1.services.ingestor_livetiming.historical import typer
from openf1.services.ingestor_livetiming.core.processing.main import process_messages

from openf1.util.db import (
    insert_data_sync,
    insert_data_async,
    get_ingestion_log_sync,
    get_ingestion_log_async,
    write_ingestion_log_sync,
    write_ingestion_log_async,
    complete_ingestion_log_sync,
    complete_ingestion_log_async,
    delete_ingestion_log_sync,
    delete_ingestion_log_async,
    delete_session_data_sync,
    delete_session_data_async,
)
from openf1.util.misc import join_url, json_serializer, to_datetime, to_timedelta
from openf1.util.schedule import get_meeting_keys
from openf1.util.schedule import get_schedule as _get_schedule
from openf1.util.schedule import get_session_keys

cli = typer.Typer()
http_client_async = None

# Flag to determine if the script is being run from the command line
_is_called_from_cli = False


def get_http_client_async():
    """Creates an async HTTP client with an indefinite TTL only when called (lazy loading)"""
    global http_client_async
    if http_client_async is None:
        http_client_async = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=None)
        )
    return http_client_async


async def http_client_cleanup():
    """Closes the async HTTP client and marks it for garbage collection."""
    global http_client_async
    try:
        if http_client_async is not None:
            await http_client_async.close()
    except Exception:
        pass
    finally:
        http_client_async = None


@cli.command()
def get_schedule(year: int) -> dict:
    schedule = _get_schedule(year)

    if _is_called_from_cli:
        schedule_json = json.dumps(schedule, indent=2, default=json_serializer)
        print(schedule_json)

    return schedule


@lru_cache()
def get_session_url(year: int, meeting_key: int, session_key: int) -> str:
    """Retrieves the URL for downloading raw data of a specific session"""
    BASE_URL = "https://livetiming.formula1.com/static"

    schedule = _get_schedule(year)

    session_url = None
    for meeting in schedule["Meetings"]:
        if meeting["Key"] == meeting_key:
            for session in meeting["Sessions"]:
                if session["Key"] == session_key:
                    if "Path" not in session:
                        continue
                    path = session["Path"]
                    session_url = join_url(BASE_URL, path)

    if session_url is None:
        raise ValueError(
            f"Session not found (year: `{year}`, meeting_key: `{meeting_key}`, "
            f"session_key: `{session_key}`)"
        )

    return session_url


def _list_topics(session_url: str) -> list[str]:
    """Returns all the available raw data filenames for the session"""
    index_url = join_url(session_url, "Index.json")
    index_response = requests.get(index_url)
    index_content = json.loads(index_response.content)

    filenames = [v["StreamPath"] for v in index_content["Feeds"].values()]
    topics = [f[: -len(".jsonStream")] for f in filenames if f.endswith(".jsonStream")]
    topics = sorted(topics)

    return topics


@cli.command()
def list_topics(
    year: int,
    meeting_key: int,
    session_key: int,
) -> list[str]:
    session_url = get_session_url(
        year=year, meeting_key=meeting_key, session_key=session_key
    )
    topics = _list_topics(session_url)

    if _is_called_from_cli:
        print(topics)
    return topics


@lru_cache()
def _get_topic_content(session_url: str, topic: str) -> list[str]:
    topic_filename = f"{topic}.jsonStream"
    url_topic = join_url(session_url, topic_filename)
    topic_content = requests.get(url_topic).text.split("\r\n")

    return topic_content


@alru_cache()
async def _get_topic_content_async(session_url: str, topic: str):
    topic_filename = f"{topic}.jsonStream"
    url_topic = join_url(session_url, topic_filename)

    response = await get_http_client_async().get(url_topic)
    topic_content = await response.text()

    return topic_content.split("\r\n")


@cli.command()
def get_topic_content(
    year: int, meeting_key: int, session_key: int, topic: str
) -> list[str]:
    session_url = get_session_url(
        year=year, meeting_key=meeting_key, session_key=session_key
    )
    content = _get_topic_content(session_url=session_url, topic=topic)

    if _is_called_from_cli:
        print("\n".join(content))
    return content


def _parse_line(line: str) -> tuple[timedelta | None, str | None]:
    """Parses a line to extract the duration since session start and raw data.

    The line is expected to be formatted as follows:
    (duration since session start, raw data)
    """
    if len(line) == 0:
        return None, None
    pattern = r"(\d+:\d+:\d+\.\d+)(.*)"
    match = re.match(pattern, line)
    if match is None:
        return None, None
    session_time = to_timedelta(match.group(1))
    raw_data = match.group(2).strip("\r").strip('"')
    return session_time, raw_data


async def _parse_and_decode_topic_content(
    topic: str,
    topic_raw_content: list[str],
    t0: datetime,
) -> list[Message]:
    messages = []

    for line in topic_raw_content:
        session_time, content = _parse_line(line)

        if session_time is None:
            continue

        if isinstance(content, str):
            content = decode(content)

        messages.append(
            Message(
                topic=topic,
                content=content,
                timepoint=t0 + session_time,
            )
        )

    # messages are not guaranteed to be sorted
    return messages


@alru_cache()
async def _get_t0(
    session_url: str,
) -> datetime:
    """Calculates the most likely start time of a session (t0) based on
    Position and CarData messages.
    The calculation method comes from the FastF1 package (https://github.com/theOehrly/Fast-F1/blob/317bacf8c61038d7e8d0f48165330167702b349f/fastf1/core.py#L2208).
    """
    t_ref = datetime(1970, 1, 1)
    t0_candidates = []

    position_content = await _get_topic_content_async(
        session_url=session_url, topic="Position.z"
    )
    cardata_content = await _get_topic_content_async(
        session_url=session_url, topic="CarData.z"
    )

    position_messages = await _parse_and_decode_topic_content(
        topic="Position.z",
        topic_raw_content=position_content,
        t0=t_ref,
    )

    cardata_messages = await _parse_and_decode_topic_content(
        topic="CarData.z",
        topic_raw_content=cardata_content,
        t0=t_ref,
    )

    for message in position_messages:
        for record in message.content["Position"]:
            timepoint = to_datetime(record["Timestamp"])
            session_time = message.timepoint - t_ref
            t0_candidates.append(timepoint - session_time)

    for message in cardata_messages:
        for record in message.content["Entries"]:
            timepoint = to_datetime(record["Utc"])
            session_time = message.timepoint - t_ref
            t0_candidates.append(timepoint - session_time)

    t0_estimate = max(t0_candidates)
    t0_estimate = pytz.utc.localize(t0_estimate)

    return t0_estimate


@cli.command()
async def get_t0(year: int, meeting_key: int, session_key: int) -> datetime:
    session_url = get_session_url(
        year=year, meeting_key=meeting_key, session_key=session_key
    )
    t0 = await _get_t0(session_url=session_url)

    if _is_called_from_cli:
        print(t0)
    return t0


async def _get_messages(
    session_url: str,
    topics: list[str],
    t0: datetime,
    parallel: bool = False,
) -> list[Message]:
    messages = []
    if parallel:
        raw_content_topics = await asyncio.gather(
            *[
                _get_topic_content_async(session_url=session_url, topic=topic)
                for topic in topics
            ]
        )
        messages_topics = await asyncio.gather(
            *[
                _parse_and_decode_topic_content(
                    topic=topic,
                    topic_raw_content=raw_content,
                    t0=t0,
                )
                for topic, raw_content in zip(topics, raw_content_topics)
            ]
        )
        messages = [
            message for messages_topic in messages_topics for message in messages_topic
        ]
    else:
        for topic in topics:
            raw_content = _get_topic_content(
                session_url=session_url,
                topic=topic,
            )
            messages += await _parse_and_decode_topic_content(
                topic=topic,
                topic_raw_content=raw_content,
                t0=t0,
            )

    messages = sorted(messages, key=lambda m: (m.timepoint, m.topic))

    return messages


@cli.command()
async def get_messages(
    year: int,
    meeting_key: int,
    session_key: int,
    topics: list[str],
    parallel: bool = False,
    verbose: bool = True,
) -> list[Message]:
    session_url = get_session_url(
        year=year, meeting_key=meeting_key, session_key=session_key
    )
    if verbose:
        logger.info(f"Session URL: {session_url} for session {session_key}")

    t0 = await _get_t0(session_url=session_url)
    if verbose:
        logger.info(f"t0: {t0} for session {session_key}")

    messages = await _get_messages(
        session_url=session_url, topics=topics, t0=t0, parallel=parallel
    )
    if verbose:
        logger.info(f"Fetched {len(messages)} messages for session {session_key}")

    if _is_called_from_cli:
        messages_json = json.dumps(messages, indent=2, default=json_serializer)
        print(messages_json)

    return messages


async def _get_processed_documents(
    year: int,
    meeting_key: int,
    session_key: int,
    collection_names: list[str],
    parallel: bool = False,
    verbose: bool = True,
) -> dict[str, list[Document]]:
    session_url = get_session_url(
        year=year, meeting_key=meeting_key, session_key=session_key
    )
    if verbose:
        logger.info(f"Session URL: {session_url} for session {session_key}")

    t0 = await _get_t0(session_url=session_url)
    if verbose:
        logger.info(f"t0: {t0} for session {session_key}")

    topics = set().union(*[get_source_topics(n) for n in collection_names])
    topics = sorted(list(topics))
    if verbose:
        logger.info(f"Topics used: {topics} for session {session_key}")

    messages = await _get_messages(
        session_url=session_url,
        topics=topics,
        t0=t0,
        parallel=parallel,
    )
    if verbose:
        logger.info(f"Fetched {len(messages)} messages for session {session_key}")

    if verbose:
        logger.info(f"Starting processing for session {session_key}")

    docs_by_collection = process_messages(
        messages=messages, meeting_key=meeting_key, session_key=session_key
    )
    docs_by_collection = {
        col: docs_by_collection[col] if col in docs_by_collection else []
        for col in collection_names
    }

    if verbose:
        n_docs = sum(len(d) for d in docs_by_collection.values())
        logger.info(f"Processed {n_docs} documents for session {session_key}")

    return docs_by_collection


@cli.command()
async def get_processed_documents(
    year: int,
    meeting_key: int,
    session_key: int,
    collection_names: list[str],
    parallel: bool = False,
    verbose: bool = True,
) -> dict[str, list[Document]]:
    docs_by_collection = await _get_processed_documents(
        year=year,
        meeting_key=meeting_key,
        session_key=session_key,
        collection_names=collection_names,
        parallel=parallel,
        verbose=verbose,
    )

    if _is_called_from_cli:
        docs_by_collection = {
            k: [d.to_mongo_doc_sync() for d in v] for k, v in docs_by_collection.items()
        }
        docs_by_collection_json = json.dumps(
            docs_by_collection, indent=2, default=json_serializer
        )
        print(docs_by_collection_json)

    return docs_by_collection


@cli.command()
async def ingest_collections(
    year: int,
    meeting_key: int,
    session_key: int,
    collection_names: list[str],
    parallel: bool = False,
    verbose: bool = True,
):
    docs_by_collection = await _get_processed_documents(
        year=year,
        meeting_key=meeting_key,
        session_key=session_key,
        collection_names=collection_names,
        parallel=parallel,
        verbose=verbose,
    )

    if verbose:
        logger.info(f"Inserting documents to DB for session {session_key}")

    if parallel:
        await asyncio.gather(
            *[
                insert_data_async(
                    collection_name=collection,
                    docs=[d.to_mongo_doc_sync() for d in docs],
                )
                for collection, docs in docs_by_collection.items()
            ]
        )
    else:
        for collection, docs in tqdm(
            list(docs_by_collection.items()), disable=not verbose
        ):
            docs_mongo = [d.to_mongo_doc_sync() for d in docs]
            insert_data_sync(collection_name=collection, docs=docs_mongo)


async def _check_and_cleanup_session(
    session_key: int,
    parallel: bool,
) -> bool:
    """Checks ingestion log for a session. Returns True if session should be skipped.
    Cleans up partial data from interrupted sessions."""
    if parallel:
        log_entry = await get_ingestion_log_async(session_key)
    else:
        log_entry = get_ingestion_log_sync(session_key)

    if log_entry is None:
        return False

    if log_entry["status"] == "completed":
        logger.info(f"Skipping session {session_key} - already ingested")
        return True

    # status == "started" — interrupted session, clean up
    logger.warning(
        f"Session {session_key} was interrupted - cleaning up partial data"
    )
    collection_names = log_entry.get("collection_names", [])
    if parallel:
        await delete_session_data_async(session_key, collection_names)
        await delete_ingestion_log_async(session_key)
    else:
        delete_session_data_sync(session_key, collection_names)
        delete_ingestion_log_sync(session_key)

    return False


def _build_ingestion_log_entry(
    session_key: int,
    meeting_key: int,
    year: int,
    collection_names: list[str],
    parallel: bool,
) -> dict:
    """Builds an ingestion log entry dict."""
    return {
        "session_key": session_key,
        "meeting_key": meeting_key,
        "year": year,
        "status": "started",
        "started_at": datetime.now(timezone.utc),
        "completed_at": None,
        "collection_names": collection_names,
        "parallel": parallel,
    }


@cli.command()
async def ingest_session(
    year: int,
    meeting_key: int,
    session_key: int,
    parallel: bool = False,
    resume: bool = False,
    verbose: bool = True,
):
    if verbose:
        logger.info(f"Ingesting session {session_key}")

    # Resume check
    if resume:
        should_skip = await _check_and_cleanup_session(
            session_key=session_key, parallel=parallel
        )
        if should_skip:
            return

    collections = get_collections(meeting_key=meeting_key, session_key=session_key)
    collection_names = sorted([c.__class__.name for c in collections])

    if verbose:
        logger.info(
            f"Ingesting {len(collection_names)} collections: {collection_names}"
        )

    # Write "started" log entry (always, regardless of --resume)
    log_entry = _build_ingestion_log_entry(
        session_key=session_key,
        meeting_key=meeting_key,
        year=year,
        collection_names=collection_names,
        parallel=parallel,
    )
    if parallel:
        await write_ingestion_log_async(log_entry)
    else:
        write_ingestion_log_sync(log_entry)

    try:
        await ingest_collections(
            year=year,
            meeting_key=meeting_key,
            session_key=session_key,
            collection_names=collection_names,
            parallel=parallel,
            verbose=verbose,
        )

        # Mark as completed
        if parallel:
            await complete_ingestion_log_async(session_key)
        else:
            complete_ingestion_log_sync(session_key)

        if verbose:
            logger.info(f"Session {session_key} ingestion completed successfully")
    except Exception:
        logger.exception(f"Session {session_key} ingestion failed")
        raise


@cli.command()
async def ingest_meeting(
    year: int,
    meeting_key: int,
    parallel: bool = False,
    by_session: bool = False,
    resume: bool = False,
    verbose: bool = True,
):
    if verbose:
        logger.info(f"Ingesting meeting {meeting_key}")

    session_keys = get_session_keys(year=year, meeting_key=meeting_key)

    if verbose:
        logger.info(f"{len(session_keys)} sessions found: {session_keys}")

    if parallel and not by_session:
        await asyncio.gather(
            *[
                ingest_session(
                    year=year,
                    meeting_key=meeting_key,
                    session_key=session_key,
                    parallel=parallel,
                    resume=resume,
                    verbose=verbose,
                )
                for session_key in session_keys
            ]
        )
    else:
        for session_key in session_keys:
            await ingest_session(
                year=year,
                meeting_key=meeting_key,
                session_key=session_key,
                parallel=parallel,
                resume=resume,
                verbose=verbose,
            )


@cli.command()
async def ingest_season(
    year: int,
    parallel: bool = False,
    by_meeting: bool = False,
    resume: bool = False,
    verbose: bool = True,
):
    meeting_keys = get_meeting_keys(year)
    if verbose:
        logger.info(f"{len(meeting_keys)} meetings found: {meeting_keys}")

    if parallel and not by_meeting:
        await asyncio.gather(
            *[
                ingest_meeting(
                    year=year,
                    meeting_key=meeting_key,
                    parallel=parallel,
                    resume=resume,
                    verbose=verbose,
                )
                for meeting_key in meeting_keys
            ]
        )
    else:
        for meeting_key in meeting_keys:
            await ingest_meeting(
                year=year,
                meeting_key=meeting_key,
                parallel=parallel,
                resume=resume,
                verbose=verbose,
            )


if __name__ == "__main__":
    _is_called_from_cli = True
    # might encounter aiohttp/asyncio complaints, but cleanup is done in a separate event loop
    cli.add_event_handler(typer.ON_EXIT, http_client_cleanup)
    cli()
