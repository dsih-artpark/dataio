# Contributing

Contributions are welcome and are greatly appreciated! Every little bit helps, and credit will always be given.

You can contribute to the project in the following ways:

- Report bugs and errors
- Suggest improvements and new features
- Write documentation
- Write code
- Write tests

## Writing code

To write code, you can use the following steps:

1. Fork the repository into your own GitHub account/organization.
2. Clone the repository to your local machine and checkout a new branch for the contribution you're working on.
3. Make your changes and commit them.
4. Push your changes to the relevant branch in your fork.
5. Create a pull request on GitHub to the development branch of the main repository. Pull requests to the production branch will not be accepted.

It is recommended to use a virtual environment to write code. The project uses uv for this purpose. Checkout the [official installation guide](https://docs.astral.sh/uv/getting-started/installation/) for more information.

```sh
uv init
uv venv
uv sync
```

```sh
uv run dataio init
uv run dataio list-datasets
uv run dataio download-dataset TS0001DS9999
```

## Writing Documentation

Documentation is always welcome! You can contribute to the documentation by writing documentation for the code you write or by improving the existing documentation.

Documentation is written in the `docs/source` folder and is written in [`MyST Markdown`](https://myst-parser.readthedocs.io/en/latest/), which is a dialect of Markdown that is compatible with Sphinx.

If you're writing docs, make sure to install the docs dependencies.

```sh
uv sync --with docs
```

You can then build the documentation locally using sphinx.

```sh
uv run sphinx-autobuild -b html docs/source docs/build
```

This command will automatically rebuild the documentation when you make changes to the source files at the 8000 port (http://localhost:8000).

To clean the documentation and rebuild it, you can either run:

```sh
rm -rf docs/build
uv run sphinx-autobuild -b html docs/source docs/build
```

or you can have sphinx do it for you:

```sh
uv run sphinx-autobuild -E -b html docs/source docs/build
```
