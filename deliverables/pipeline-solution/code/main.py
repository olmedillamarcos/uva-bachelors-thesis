from pathlib import Path
from staffspy import LinkedInAccount, SolverType, DriverType, BrowserType
import json
import pandas as pd

def main():
    with open("user_ids copy.json", "r", encoding="utf-8") as f:

        user_ids = json.load(f)

    session_file = Path().resolve()/  'session.pkl'

    account = LinkedInAccount(
        session_file=str(session_file),
        log_level=1,
    )

    users = account.scrape_users(
        user_ids=user_ids
    )

    all_dfs = []
        # 4️⃣ Extract ID(s) and scrape, skipping failures
    for urls_cell in user_ids:
        try:
            # extract ID after '/in/'
            user_id = urls_cell
            # scrape returns a DataFrame
            res_df = account.scrape_users(user_ids=[user_id])
            all_dfs.append(res_df)
        except Exception:
            continue

    users = pd.concat(all_dfs)

    users.to_csv("users_1335-1434.csv", index=False)

if __name__ == "__main__":
    main()
    