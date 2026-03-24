import argparse
import subprocess

import pymongo

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, required=True)
    args = parser.parse_args()

    client = pymongo.MongoClient("mongodb://localhost:27017")
    db = client["openf1-livetiming"]
    sessions = db.sessions.find({"year": args.year})

    for s in sessions:
        mk = s["meeting_key"]
        sk = s["session_key"]
        st = s["session_name"]  # usually Qualifying, Race, Practice 1, etc.

        print(f"Ingesting session result for meeting {mk} session {sk} - {st}")
        subprocess.run(
            [
                "python",
                "-m",
                "openf1.services.f1_scraping.session_result",
                "--meeting-key",
                str(mk),
                "--session-key",
                str(sk),
            ]
        )

        if "Qualifying" in st:
            print(f"Ingesting starting grid for meeting {mk} session {sk} - {st}")
            subprocess.run(
                [
                    "python",
                    "-m",
                    "openf1.services.f1_scraping.starting_grid",
                    "--meeting-key",
                    str(mk),
                    "--session-key",
                    str(sk),
                ]
            )

    print("Done ingesting static scraped results!")
