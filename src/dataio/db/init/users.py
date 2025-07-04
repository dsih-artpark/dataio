import os
import csv
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json
from psycopg2.extensions import register_adapter
import secrets
import bcrypt
import argparse

register_adapter(dict, Json)

# Load environment variables from .env file
load_dotenv()

# Database connection parameters from .env
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
REPO_DIR = os.getenv("REPO_DIR")


def parse_args():
    parser = argparse.ArgumentParser(description="Insert test users into the database")
    parser.add_argument(
        "-u",
        "--add-test-users",
        action="store_true",
        help="Add test users to the database",
    )
    parser.add_argument(
        "-g", "--add-groups", action="store_true", help="Add groups to the database"
    )
    parser.add_argument(
        "-r",
        "--add-resource-groups",
        action="store_true",
        help="Add resource groups to the database",
    )
    parser.add_argument(
        "-a",
        "--add-user-permissions",
        action="store_true",
        help="Add user permissions to the database",
    )
    return parser.parse_args()


def connect_to_db():
    """Establish connection to the database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
        )
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        raise


def generate_user(email, is_admin):
    key = secrets.token_urlsafe()
    bytes = key.encode("utf-8")
    salt = bcrypt.gensalt()
    hash = bcrypt.hashpw(bytes, salt)
    return {
        "email": email,
        "key": hash,
        "is_group": False,
        "unhashed_key": key,
        "is_admin": is_admin,
    }


def generate_group(email):
    return {"email": email, "is_group": True, "key": None, "is_admin": False}


def add_test_users():
    conn = connect_to_db()
    cur = conn.cursor()
    test_users = [generate_user("admin@artpark.in", True)]

    print(test_users)

    for user in test_users:
        cur.execute(
            """INSERT INTO users (email, key, is_group, is_admin) VALUES (%s, %s, %s, %s)""",
            (user["email"], user["key"], user["is_group"], user["is_admin"]),
        )

    conn.commit()
    print("All test users inserted successfully!")
    # put the test_users into a csv file
    os.makedirs(f"{REPO_DIR}/db/init/data_inserts", exist_ok=True)
    with open(f"{REPO_DIR}/db/init/data_inserts/test_users.csv", "w") as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=["email", "key", "is_group", "unhashed_key", "is_admin"]
        )
        writer.writeheader()
        writer.writerows(test_users)


def add_groups():
    conn = connect_to_db()
    cur = conn.cursor()
    group_mapping = {}
    for group, users in group_mapping.items():
        for user in users:
            cur.execute(
                """INSERT INTO user_groups (group_email, user_email) VALUES (%s, %s)""",
                (group, user),
            )
    conn.commit()
    print("All groups inserted successfully!")


def add_resource_groups():
    resource_group = {
        "group_id": "INLEAD_LIVESTOCK",
        "group_name": "Inlead Livestock Modelling Group",
        "resources": [
            {
                "resource_type": "DATASET",
                "resource_id": "CS0007DS0041",
                "resource_json": {},
            },
            {
                "resource_type": "DATASET",
                "resource_id": "EP0006DS0055",
                "resource_json": {},
            },
            {
                "resource_type": "DATASET",
                "resource_id": "CS0007DS0083",
                "resource_json": {},
            },
            {
                "resource_type": "DATASET",
                "resource_id": "CS0002DS0084",
                "resource_json": {},
            },
        ],
    }

    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute(
        """INSERT INTO resource_groups (resource_group_id, group_name) VALUES (%s, %s) ON CONFLICT DO NOTHING""",
        (resource_group["group_id"], resource_group["group_name"]),
    )
    conn.commit()
    print("Resource group inserted successfully!")

    # insert resources into resource_group_members
    for resource in resource_group["resources"]:
        cur.execute(
            """select add_resource_group_member(%s, %s, %s, %s)""",
            (
                resource_group["group_name"],
                resource["resource_id"],
                resource["resource_type"],
                resource["resource_json"],
            ),
        )
    conn.commit()


def add_user_permissions():
    user_permissions = [
        # {
        #     "user_email": "admin@artpark.in",
        #     "resource_type": "DATASET",
        #     "resource_id": "GS0012DS0051",
        #     "permission": "DOWNLOAD",
        # },
        # {
        #     "user_email": "analyst@artpark.in",
        #     "resource_type": "GROUP",
        #     "resource_id": "livestock_group",
        #     "permission": "DOWNLOAD",
        # },
        # {
        #     "user_email": "external_collaborator@psu.edu",
        #     "resource_type": "GROUP",
        #     "resource_id": "livestock_group",
        #     "permission": "DOWNLOAD",
        # },
        # {
        #     "user_email": "public_user@artpark.in",
        #     "resource_type": "GROUP",
        #     "resource_id": "livestock_group",
        #     "permission": "VIEW",
        # },
    ]

    conn = connect_to_db()
    cur = conn.cursor()
    for user_permission in user_permissions:
        cur.execute(
            """INSERT INTO user_permissions (user_email, resource_type, resource_id, permission) 
               VALUES (%s, %s, %s, %s)""",
            (
                user_permission["user_email"],
                user_permission["resource_type"],
                user_permission["resource_id"],
                user_permission["permission"],
            ),
        )
    conn.commit()
    print("User permissions inserted successfully!")


if __name__ == "__main__":
    args = parse_args()
    if args.add_test_users:
        add_test_users()
    if args.add_groups:
        add_groups()
    if args.add_resource_groups:
        add_resource_groups()
    if args.add_user_permissions:
        add_user_permissions()
