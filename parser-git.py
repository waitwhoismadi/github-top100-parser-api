import requests
import psycopg2
from datetime import datetime, timedelta

def get_db_connection():
    conn = psycopg2.connect(
        dbname="top100",
        user="postgres",
        password="admin123",
        host="localhost",
        port="5432"
    )
    return conn

def fetch_top_repos():
    url = "https://api.github.com/search/repositories?q=stars:>0&sort=stars&order=desc&per_page=100"
    response = requests.get(url)
    return response.json()["items"]

def save_repos_to_db(repos):
    conn = get_db_connection()
    cursor = conn.cursor()
    for i, repo in enumerate(repos):
        cursor.execute("""
            INSERT INTO repos (repo, owner, position_cur, position_prev, stars, watchers, forks, open_issues, language)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (repo, owner) DO UPDATE SET
            position_cur = EXCLUDED.position_cur,
            position_prev = repos.position_cur,
            stars = EXCLUDED.stars,
            watchers = EXCLUDED.watchers,
            forks = EXCLUDED.forks,
            open_issues = EXCLUDED.open_issues,
            language = EXCLUDED.language;
        """, (
            repo["full_name"], repo["owner"]["login"], i + 1, None, repo["stargazers_count"],
            repo["watchers_count"], repo["forks_count"], repo["open_issues_count"], repo["language"]
        ))
    conn.commit()
    cursor.close()
    conn.close()

def fetch_repo_activity(owner, repo, since, until):
    url = f"https://api.github.com/repos/{owner}/{repo}/commits"
    params = {"since": since, "until": until}
    response = requests.get(url, params=params)
    return response.json()

def save_repo_activity_to_db(owner, repo, activity):
    conn = get_db_connection()
    cursor = conn.cursor()
    for commit in activity:
        date = commit["commit"]["author"]["date"][:10]
        authors = [commit["commit"]["author"]["name"]]
        cursor.execute("""
            INSERT INTO repo_activity (owner, repo, date, commits, authors)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (owner, repo, date) DO UPDATE SET
            commits = repo_activity.commits + 1,
            authors = array_append(repo_activity.authors, EXCLUDED.authors[1])
        """, (owner, repo, date, 1, authors))
    conn.commit()
    cursor.close()
    conn.close()

def main():
    repos = fetch_top_repos()
    save_repos_to_db(repos)

    since = (datetime.now() - timedelta(days=7)).isoformat()
    until = datetime.now().isoformat()

    for repo in repos:
        owner, repo_name = repo["full_name"].split("/")
        activity = fetch_repo_activity(owner, repo_name, since, until)
        save_repo_activity_to_db(owner, repo_name, activity)

if __name__ == "__main__":
    main()