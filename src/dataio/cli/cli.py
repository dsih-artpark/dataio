import typer

from dataio.cli.usage import usage_app
from dataio.cli.user import app as user_app

app = typer.Typer(name="dataio")

for command in user_app.registered_commands:
    app.registered_commands.append(command)

# Add the user app to the root app
app.add_typer(
    user_app,
    name="user",
    help="This app can be used to interact with the user API endpoints explicitly. "
    "Using this sub-app is optional, and the recommended way to interact with the root dataio command.",
)

# Add usage tracking commands
app.add_typer(
    usage_app,
    name="usage",
    help="Usage tracking commands for viewing and managing API usage statistics.",
)

if __name__ == "__main__":
    app()
