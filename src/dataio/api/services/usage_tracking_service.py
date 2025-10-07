import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from .base_service import BaseService


class UsageTrackingService(BaseService):
    """Service for tracking API usage and logging to SQLite database."""

    def __init__(self, db_path: str = "usage_tracking.db"):
        super().__init__()
        self.db_path = db_path
        self._init_database()

    def _init_database(self):
        """Initialize SQLite database with usage tracking table."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS usage_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    datetime TEXT NOT NULL,
                    user_email TEXT NOT NULL,
                    method_type TEXT NOT NULL,
                    endpoint TEXT NOT NULL,
                    http_method TEXT NOT NULL,
                    dataset_id TEXT,
                    bucket_type TEXT,
                    region_id TEXT,
                    user_agent TEXT,
                    ip_address TEXT,
                    response_status INTEGER DEFAULT 200,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes for better performance
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_usage_logs_user 
                ON usage_logs(user_email)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_usage_logs_datetime 
                ON usage_logs(datetime)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_usage_logs_dataset 
                ON usage_logs(dataset_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_usage_logs_endpoint 
                ON usage_logs(endpoint)
            """)

            conn.commit()
            conn.close()

            self.logger.info(f"Usage tracking database initialized at {self.db_path}")

        except Exception as e:
            self.logger.error(f"Error initializing usage tracking database: {str(e)}")
            raise

    def log_usage(
        self,
        user_email: str,
        method_type: str,
        endpoint: str,
        http_method: str,
        dataset_id: Optional[str] = None,
        bucket_type: Optional[str] = None,
        region_id: Optional[str] = None,
        user_agent: Optional[str] = None,
        ip_address: Optional[str] = None,
        response_status: int = 200,
    ):
        """Log usage to SQLite database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            timestamp = datetime.now(timezone.utc).isoformat()

            cursor.execute(
                """
                INSERT INTO usage_logs 
                (datetime, user_email, method_type, endpoint, http_method,
                 dataset_id, bucket_type, region_id, user_agent, ip_address, response_status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    timestamp,
                    user_email,
                    method_type,
                    endpoint,
                    http_method,
                    dataset_id,
                    bucket_type,
                    region_id,
                    user_agent,
                    ip_address,
                    response_status,
                ),
            )

            conn.commit()
            conn.close()

            self.logger.debug(
                f"Usage logged: {user_email} - {method_type} - {endpoint}"
            )

        except Exception as e:
            self.logger.error(f"Error logging usage: {str(e)}")

    def get_usage_stats(
        self, user_email: Optional[str] = None, days: int = 30
    ) -> Dict[str, Any]:
        """Get usage statistics from database."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Calculate cutoff date
            cutoff_date = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            cutoff_date = cutoff_date.replace(day=cutoff_date.day - days)
            cutoff_str = cutoff_date.isoformat()

            # Base query
            base_query = """
                SELECT method_type, endpoint, user_email, dataset_id, response_status
                FROM usage_logs 
                WHERE datetime > ?
            """
            params = [cutoff_str]

            if user_email:
                base_query += " AND user_email = ?"
                params.append(user_email)

            cursor.execute(base_query, params)
            rows = cursor.fetchall()

            # Process results
            stats = {
                "total_requests": len(rows),
                "by_method": {},
                "by_endpoint": {},
                "by_user": {},
                "by_dataset": {},
                "by_status": {},
                "success_rate": 0,
            }

            successful_requests = 0

            for row in rows:
                method_type, endpoint, user, dataset_id, response_status = row

                # Count by method
                stats["by_method"][method_type] = (
                    stats["by_method"].get(method_type, 0) + 1
                )

                # Count by endpoint
                stats["by_endpoint"][endpoint] = (
                    stats["by_endpoint"].get(endpoint, 0) + 1
                )

                # Count by user
                stats["by_user"][user] = stats["by_user"].get(user, 0) + 1

                # Count by dataset
                if dataset_id:
                    stats["by_dataset"][dataset_id] = (
                        stats["by_dataset"].get(dataset_id, 0) + 1
                    )

                # Count by status
                stats["by_status"][str(response_status)] = (
                    stats["by_status"].get(str(response_status), 0) + 1
                )

                # Count successful requests
                if 200 <= response_status < 300:
                    successful_requests += 1

            # Calculate success rate
            if stats["total_requests"] > 0:
                stats["success_rate"] = round(
                    (successful_requests / stats["total_requests"]) * 100, 2
                )

            conn.close()
            return stats

        except Exception as e:
            self.logger.error(f"Error getting usage stats: {str(e)}")
            return {"error": str(e)}

    def get_user_activity(self, user_email: str, days: int = 30) -> Dict[str, Any]:
        """Get detailed activity for a specific user."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cutoff_date = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            cutoff_date = cutoff_date.replace(day=cutoff_date.day - days)
            cutoff_str = cutoff_date.isoformat()

            # Get user activity
            cursor.execute(
                """
                SELECT datetime, method_type, endpoint, http_method, dataset_id, 
                       bucket_type, region_id, response_status
                FROM usage_logs 
                WHERE user_email = ? AND datetime > ?
                ORDER BY datetime DESC
                LIMIT 100
            """,
                (user_email, cutoff_str),
            )

            rows = cursor.fetchall()

            activity = {
                "user_email": user_email,
                "total_requests": len(rows),
                "recent_activity": [],
            }

            for row in rows:
                activity["recent_activity"].append(
                    {
                        "datetime": row[0],
                        "method_type": row[1],
                        "endpoint": row[2],
                        "http_method": row[3],
                        "dataset_id": row[4],
                        "bucket_type": row[5],
                        "region_id": row[6],
                        "response_status": row[7],
                    }
                )

            conn.close()
            return activity

        except Exception as e:
            self.logger.error(f"Error getting user activity: {str(e)}")
            return {"error": str(e)}

    def get_dataset_usage(self, dataset_id: str, days: int = 30) -> Dict[str, Any]:
        """Get usage statistics for a specific dataset."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cutoff_date = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            cutoff_date = cutoff_date.replace(day=cutoff_date.day - days)
            cutoff_str = cutoff_date.isoformat()

            # Get dataset usage
            cursor.execute(
                """
                SELECT user_email, method_type, datetime, response_status
                FROM usage_logs 
                WHERE dataset_id = ? AND datetime > ?
                ORDER BY datetime DESC
            """,
                (dataset_id, cutoff_str),
            )

            rows = cursor.fetchall()

            usage = {
                "dataset_id": dataset_id,
                "total_downloads": len(rows),
                "unique_users": len(set(row[0] for row in rows)),
                "by_method": {},
                "by_user": {},
                "recent_downloads": [],
            }

            for row in rows:
                user_email, method_type, datetime_str, response_status = row

                # Count by method
                usage["by_method"][method_type] = (
                    usage["by_method"].get(method_type, 0) + 1
                )

                # Count by user
                usage["by_user"][user_email] = usage["by_user"].get(user_email, 0) + 1

                # Add to recent downloads (limit to 20)
                if len(usage["recent_downloads"]) < 20:
                    usage["recent_downloads"].append(
                        {
                            "user_email": user_email,
                            "method_type": method_type,
                            "datetime": datetime_str,
                            "response_status": response_status,
                        }
                    )

            conn.close()
            return usage

        except Exception as e:
            self.logger.error(f"Error getting dataset usage: {str(e)}")
            return {"error": str(e)}

    def export_to_csv(
        self, output_file: str, user_email: Optional[str] = None, days: int = 30
    ) -> int:
        """Export usage data to CSV file."""
        import csv

        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cutoff_date = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            cutoff_date = cutoff_date.replace(day=cutoff_date.day - days)
            cutoff_str = cutoff_date.isoformat()

            query = """
                SELECT datetime, user_email, method_type, endpoint, http_method,
                       dataset_id, bucket_type, region_id, user_agent, ip_address, response_status
                FROM usage_logs 
                WHERE datetime > ?
            """
            params = [cutoff_str]

            if user_email:
                query += " AND user_email = ?"
                params.append(user_email)

            query += " ORDER BY datetime DESC"

            cursor.execute(query, params)
            rows = cursor.fetchall()

            with open(output_file, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(
                    [
                        "datetime",
                        "user_email",
                        "method_type",
                        "endpoint",
                        "http_method",
                        "dataset_id",
                        "bucket_type",
                        "region_id",
                        "user_agent",
                        "ip_address",
                        "response_status",
                    ]
                )
                writer.writerows(rows)

            conn.close()
            return len(rows)

        except Exception as e:
            self.logger.error(f"Error exporting to CSV: {str(e)}")
            return 0

    def cleanup_old_data(self, days_to_keep: int = 365):
        """Remove old usage data to manage database size."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cutoff_date = datetime.now(timezone.utc).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            cutoff_date = cutoff_date.replace(day=cutoff_date.day - days_to_keep)
            cutoff_str = cutoff_date.isoformat()

            cursor.execute("DELETE FROM usage_logs WHERE datetime < ?", (cutoff_str,))
            deleted_count = cursor.rowcount

            conn.commit()
            conn.close()

            self.logger.info(f"Cleaned up {deleted_count} old usage records")
            return deleted_count

        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {str(e)}")
            return 0
