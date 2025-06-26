from pathlib import Path
from staffspy import LinkedInAccount, SolverType, DriverType, BrowserType
import json
import pandas as pd


session_file = Path(__file__).resolve().parent /  'session.pkl'

with open("user_ids.json", "r", encoding="utf-8") as f:
    user_ids = json.load(f)

if __name__ == "__main__":
    
    account = LinkedInAccount(
        session_file=str(session_file),
        log_level=1
    )

    all_dfs = []
    for user in user_ids:
        try:
            res_df = account.scrape_users(user_ids=[user])
            all_dfs.append(res_df)
        except Exception:
            continue

    users = pd.concat(all_dfs)
    users.to_csv("users_1435-1534.csv", index=False)
