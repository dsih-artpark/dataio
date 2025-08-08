import typer

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

if __name__ == "__main__":
    app()
