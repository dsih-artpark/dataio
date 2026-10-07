import typer

from dataio.cli.draft import app as draft_app
from dataio.cli.user import app as user_app
from dataio.cli.validate import app as validate_app

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
app.add_typer(validate_app, name="validate", help="Validate manifests and data files locally.")
app.add_typer(draft_app, name="draft", help="Draft dataset metadata.yaml using an LLM, for curator review.")

if __name__ == "__main__":
    app()
