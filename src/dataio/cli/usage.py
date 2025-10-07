import os
from typing import Optional

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from dataio.api.services.usage_tracking_service import UsageTrackingService

console = Console()
usage_app = typer.Typer(name="usage", help="Usage tracking commands")


@usage_app.command("stats")
def usage_stats(
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Filter by specific user email"
    ),
    days: int = typer.Option(30, "--days", "-d", help="Number of days to look back"),
    db_file: str = typer.Option(
        "usage_tracking.db", "--db", help="Path to usage tracking database file"
    ),
):
    """Show usage statistics from the tracking database."""

    if not os.path.exists(db_file):
        console.print(f"[red]Usage tracking database not found: {db_file}[/red]")
        console.print(
            "Make sure the API server is running and has processed some requests."
        )
        return

    usage_service = UsageTrackingService(db_file)
    stats = usage_service.get_usage_stats(user, days)

    if "error" in stats:
        console.print(f"[red]Error reading usage stats: {stats['error']}[/red]")
        return

    # Display overall stats
    rprint(f"\n[bold blue]Usage Statistics (Last {days} days)[/bold blue]")
    rprint(f"Total Requests: [bold green]{stats['total_requests']}[/bold green]")
    rprint(f"Success Rate: [bold green]{stats['success_rate']}%[/bold green]")

    # Display by method type
    if stats["by_method"]:
        rprint("\n[bold]By Method Type:[/bold]")
        method_table = Table(show_header=True, header_style="bold magenta")
        method_table.add_column("Method", style="cyan")
        method_table.add_column("Count", justify="right", style="green")
        method_table.add_column("Percentage", justify="right", style="yellow")

        total = stats["total_requests"]
        for method, count in sorted(
            stats["by_method"].items(), key=lambda x: x[1], reverse=True
        ):
            percentage = round((count / total) * 100, 1) if total > 0 else 0
            method_table.add_row(method, str(count), f"{percentage}%")

        console.print(method_table)

    # Display by endpoint
    if stats["by_endpoint"]:
        rprint("\n[bold]By Endpoint:[/bold]")
        endpoint_table = Table(show_header=True, header_style="bold magenta")
        endpoint_table.add_column("Endpoint", style="cyan")
        endpoint_table.add_column("Count", justify="right", style="green")
        endpoint_table.add_column("Percentage", justify="right", style="yellow")

        for endpoint, count in sorted(
            stats["by_endpoint"].items(), key=lambda x: x[1], reverse=True
        )[:10]:
            percentage = round((count / total) * 100, 1) if total > 0 else 0
            endpoint_table.add_row(endpoint, str(count), f"{percentage}%")

        console.print(endpoint_table)

    # Display by user (if not filtering by specific user)
    if not user and stats["by_user"]:
        rprint("\n[bold]By User:[/bold]")
        user_table = Table(show_header=True, header_style="bold magenta")
        user_table.add_column("User", style="cyan")
        user_table.add_column("Count", justify="right", style="green")
        user_table.add_column("Percentage", justify="right", style="yellow")

        for user_email, count in sorted(
            stats["by_user"].items(), key=lambda x: x[1], reverse=True
        )[:10]:
            percentage = round((count / total) * 100, 1) if total > 0 else 0
            user_table.add_row(user_email, str(count), f"{percentage}%")

        console.print(user_table)

    # Display by dataset
    if stats["by_dataset"]:
        rprint("\n[bold]By Dataset:[/bold]")
        dataset_table = Table(show_header=True, header_style="bold magenta")
        dataset_table.add_column("Dataset ID", style="cyan")
        dataset_table.add_column("Downloads", justify="right", style="green")
        dataset_table.add_column("Percentage", justify="right", style="yellow")

        for dataset_id, count in sorted(
            stats["by_dataset"].items(), key=lambda x: x[1], reverse=True
        )[:10]:
            percentage = round((count / total) * 100, 1) if total > 0 else 0
            dataset_table.add_row(dataset_id, str(count), f"{percentage}%")

        console.print(dataset_table)

    # Display by status
    if stats["by_status"]:
        rprint("\n[bold]By Response Status:[/bold]")
        status_table = Table(show_header=True, header_style="bold magenta")
        status_table.add_column("Status", style="cyan")
        status_table.add_column("Count", justify="right", style="green")
        status_table.add_column("Percentage", justify="right", style="yellow")

        for status, count in sorted(
            stats["by_status"].items(), key=lambda x: int(x[0])
        ):
            percentage = round((count / total) * 100, 1) if total > 0 else 0
            status_table.add_row(status, str(count), f"{percentage}%")

        console.print(status_table)


@usage_app.command("user")
def user_activity(
    user_email: str = typer.Argument(..., help="User email to get activity for"),
    days: int = typer.Option(30, "--days", "-d", help="Number of days to look back"),
    db_file: str = typer.Option(
        "usage_tracking.db", "--db", help="Path to usage tracking database file"
    ),
):
    """Show detailed activity for a specific user."""

    if not os.path.exists(db_file):
        console.print(f"[red]Usage tracking database not found: {db_file}[/red]")
        return

    usage_service = UsageTrackingService(db_file)
    activity = usage_service.get_user_activity(user_email, days)

    if "error" in activity:
        console.print(f"[red]Error reading user activity: {activity['error']}[/red]")
        return

    rprint(f"\n[bold blue]User Activity: {user_email}[/bold blue]")
    rprint(
        f"Total Requests (Last {days} days): [bold green]{activity['total_requests']}[/bold green]"
    )

    if activity["recent_activity"]:
        rprint("\n[bold]Recent Activity:[/bold]")
        activity_table = Table(show_header=True, header_style="bold magenta")
        activity_table.add_column("DateTime", style="cyan")
        activity_table.add_column("Method", style="yellow")
        activity_table.add_column("Endpoint", style="green")
        activity_table.add_column("Dataset", style="blue")
        activity_table.add_column("Status", justify="right", style="red")

        for activity_item in activity["recent_activity"][
            :20
        ]:  # Show last 20 activities
            activity_table.add_row(
                activity_item["datetime"][:19],  # Truncate to remove timezone info
                activity_item["method_type"],
                activity_item["endpoint"],
                activity_item["dataset_id"] or "-",
                str(activity_item["response_status"]),
            )

        console.print(activity_table)
    else:
        console.print("[yellow]No recent activity found for this user.[/yellow]")


@usage_app.command("dataset")
def dataset_usage(
    dataset_id: str = typer.Argument(..., help="Dataset ID to get usage for"),
    days: int = typer.Option(30, "--days", "-d", help="Number of days to look back"),
    db_file: str = typer.Option(
        "usage_tracking.db", "--db", help="Path to usage tracking database file"
    ),
):
    """Show usage statistics for a specific dataset."""

    if not os.path.exists(db_file):
        console.print(f"[red]Usage tracking database not found: {db_file}[/red]")
        return

    usage_service = UsageTrackingService(db_file)
    usage = usage_service.get_dataset_usage(dataset_id, days)

    if "error" in usage:
        console.print(f"[red]Error reading dataset usage: {usage['error']}[/red]")
        return

    rprint(f"\n[bold blue]Dataset Usage: {dataset_id}[/bold blue]")
    rprint(
        f"Total Downloads (Last {days} days): [bold green]{usage['total_downloads']}[/bold green]"
    )
    rprint(f"Unique Users: [bold green]{usage['unique_users']}[/bold green]")

    if usage["by_method"]:
        rprint("\n[bold]By Method:[/bold]")
        method_table = Table(show_header=True, header_style="bold magenta")
        method_table.add_column("Method", style="cyan")
        method_table.add_column("Count", justify="right", style="green")

        for method, count in sorted(
            usage["by_method"].items(), key=lambda x: x[1], reverse=True
        ):
            method_table.add_row(method, str(count))

        console.print(method_table)

    if usage["by_user"]:
        rprint("\n[bold]By User:[/bold]")
        user_table = Table(show_header=True, header_style="bold magenta")
        user_table.add_column("User", style="cyan")
        user_table.add_column("Downloads", justify="right", style="green")

        for user_email, count in sorted(
            usage["by_user"].items(), key=lambda x: x[1], reverse=True
        )[:10]:
            user_table.add_row(user_email, str(count))

        console.print(user_table)

    if usage["recent_downloads"]:
        rprint("\n[bold]Recent Downloads:[/bold]")
        recent_table = Table(show_header=True, header_style="bold magenta")
        recent_table.add_column("DateTime", style="cyan")
        recent_table.add_column("User", style="yellow")
        recent_table.add_column("Method", style="green")
        recent_table.add_column("Status", justify="right", style="red")

        for download in usage["recent_downloads"]:
            recent_table.add_row(
                download["datetime"][:19],  # Truncate to remove timezone info
                download["user_email"],
                download["method_type"],
                str(download["response_status"]),
            )

        console.print(recent_table)


@usage_app.command("export")
def export_usage(
    output_file: str = typer.Option(
        "usage_export.csv", "--output", "-o", help="Output CSV file"
    ),
    db_file: str = typer.Option(
        "usage_tracking.db", "--db", help="Path to usage tracking database file"
    ),
    user: Optional[str] = typer.Option(
        None, "--user", "-u", help="Filter by specific user email"
    ),
    days: int = typer.Option(30, "--days", "-d", help="Number of days to look back"),
):
    """Export usage data to a CSV file with optional filtering."""

    if not os.path.exists(db_file):
        console.print(f"[red]Usage tracking database not found: {db_file}[/red]")
        return

    usage_service = UsageTrackingService(db_file)

    try:
        exported_count = usage_service.export_to_csv(output_file, user, days)
        console.print(
            f"[green]Exported {exported_count} records to {output_file}[/green]"
        )

        if exported_count > 0:
            file_size = os.path.getsize(output_file)
            console.print(f"File size: {file_size:,} bytes")

    except Exception as e:
        console.print(f"[red]Error exporting usage data: {str(e)}[/red]")


@usage_app.command("cleanup")
def cleanup_old_data(
    days_to_keep: int = typer.Option(
        365, "--days", "-d", help="Number of days of data to keep"
    ),
    db_file: str = typer.Option(
        "usage_tracking.db", "--db", help="Path to usage tracking database file"
    ),
    confirm: bool = typer.Option(
        False, "--confirm", help="Confirm the cleanup operation"
    ),
):
    """Clean up old usage data to manage database size."""

    if not os.path.exists(db_file):
        console.print(f"[red]Usage tracking database not found: {db_file}[/red]")
        return

    if not confirm:
        console.print(
            f"[yellow]This will delete usage data older than {days_to_keep} days.[/yellow]"
        )
        console.print(
            "[yellow]Use --confirm flag to proceed with the cleanup.[/yellow]"
        )
        return

    usage_service = UsageTrackingService(db_file)

    try:
        deleted_count = usage_service.cleanup_old_data(days_to_keep)
        console.print(f"[green]Cleaned up {deleted_count} old usage records[/green]")
        console.print(f"[green]Keeping {days_to_keep} days of data[/green]")

    except Exception as e:
        console.print(f"[red]Error cleaning up old data: {str(e)}[/red]")


@usage_app.command("info")
def database_info(
    db_file: str = typer.Option(
        "usage_tracking.db", "--db", help="Path to usage tracking database file"
    ),
):
    """Show information about the usage tracking database."""

    if not os.path.exists(db_file):
        console.print(f"[red]Usage tracking database not found: {db_file}[/red]")
        return

    try:
        import os
        import sqlite3
        from datetime import datetime

        # Get file info
        file_size = os.path.getsize(db_file)
        file_modified = datetime.fromtimestamp(os.path.getmtime(db_file))

        # Get database info
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Get total records
        cursor.execute("SELECT COUNT(*) FROM usage_logs")
        total_records = cursor.fetchone()[0]

        # Get date range
        cursor.execute("SELECT MIN(datetime), MAX(datetime) FROM usage_logs")
        date_range = cursor.fetchone()

        # Get unique users
        cursor.execute("SELECT COUNT(DISTINCT user_email) FROM usage_logs")
        unique_users = cursor.fetchone()[0]

        # Get unique datasets
        cursor.execute(
            "SELECT COUNT(DISTINCT dataset_id) FROM usage_logs WHERE dataset_id IS NOT NULL"
        )
        unique_datasets = cursor.fetchone()[0]

        conn.close()

        rprint("\n[bold blue]Usage Tracking Database Info[/bold blue]")
        rprint(f"Database File: [cyan]{db_file}[/cyan]")
        rprint(f"File Size: [green]{file_size:,} bytes[/green]")
        rprint(
            f"Last Modified: [yellow]{file_modified.strftime('%Y-%m-%d %H:%M:%S')}[/yellow]"
        )
        rprint(f"Total Records: [green]{total_records:,}[/green]")
        rprint(f"Unique Users: [green]{unique_users}[/green]")
        rprint(f"Unique Datasets: [green]{unique_datasets}[/green]")

        if date_range[0] and date_range[1]:
            rprint(
                f"Date Range: [yellow]{date_range[0][:10]} to {date_range[1][:10]}[/yellow]"
            )

    except Exception as e:
        console.print(f"[red]Error reading database info: {str(e)}[/red]")


if __name__ == "__main__":
    usage_app()
