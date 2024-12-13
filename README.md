# Multi-Storage Client

The Multi-Storage Client (MSC) is a unified high-performance Python client for object and file stores such as AWS S3, Google Cloud Storage (GCS), Oracle Cloud Infrastructure (OCI) Blog Storage, Azure Blob Storage, NVIDIA AIStore, POSIX file systems, and more.

You can use the client's generic interface to interact with objects and files across various storage services.

See the [documentation](https://nvidia.github.io/multi-storage-client) to get started.

## Layout

Important landmarks:

```text
Key:
🤖 = Generated

.
│   # GitHub templates and pipelines.
├── .github/
│   └── ...
│
│   # GitLab templates and pipelines.
├── .gitlab/
│   └── ...
│
│   # Release notes.
├── .release_notes/
│   └── ...
│
│   # Python package build outputs.
├── dist/ 🤖
│   └── ...
│
│   # Python documentation configuration.
├── docs/
│   │
│   │   # Python documentation build outputs.
│   ├── dist/ 🤖
│   │   └── ...
│   │
│   │   # Python documentation source.
│   ├── src/
│   │   └── ...
│   │
│   │   # Python documentation configuration.
│   └── conf.py
│
│   # Python package source.
├── src/
│   └── ...
│
│   # Python package test source.
├── tests/
│   │
│   │   # Unit tests.
│   ├── unit/
│   │   └── ...
│   │
│   │   # Integration tests.
│   ├── integration/
│   │   └── ...
│   │
│   │   # End-to-end (E2E) tests.
│   └── e2e/
│       └── ...
│
│   # GitLab pipeline entrypoint.
├── .gitlab-ci.yml
│
│   # Integration test containers.
├── docker-compose.yml
│
│   # Reproducible shell configuration.
├── flake.nix
├── flake.lock 🤖
│
│   # Build recipes.
├── justfile
│
│   # Python package configuration.
├── pyproject.toml
└── poetry.lock 🤖
```

## Tools

### Nix

[Nix](https://nixos.org) is a package manager and build system centered around reproducibility.

For us, Nix's most useful feature is its ability to create reproducible + isolated CLI shells on the same machine which use different versions of the same package (e.g. Java 17 and 21). Shell configurations can be encapsulated in Nix configurations which can be shared across multiple computers.

The best way to install Nix is with the [Determinate Nix Installer](https://github.com/DeterminateSystems/nix-installer) ([guide](https://zero-to-nix.com/start/install)).

Once installed, running `nix develop` in a directory with a `flake.nix` will create a nested Bash shell defined by the flake.

> If you're on a network with lots of GitHub traffic, you may get a rate limiting error. To work around this, you can either switch networks (e.g. turn off VPN) or add a GitHub personal access token (classic) to your `/etc/nix/nix.conf` (system) or `~/.config/nix/nix.conf` (user).
>
> ```text
> # https://nixos.org/manual/nix/stable/command-ref/conf-file
> access-tokens = github.com=ghp_{rest of token}
> ```

### direnv

[direnv](https://direnv.net) ([🍺](https://formulae.brew.sh/formula/direnv)) is a shell extension which can automatically load and unload environment variables when you enter or leave a specific directory.

It can automatically load and unload a Nix environment when we enter and leave a project directory.

__Unlike `nix develop` which drops you in a nested Bash shell, direnv extracts the environment variables from the nested Bash shell into your current shell (e.g. Bash, Zsh, Fish).__

Follow the [installation instructions on its website](https://direnv.net#basic-installation).

#### Editor Plugins

Plugins to add editor support for direnv. Note that these won't automatically reload the environment after you change Nix flakes unlike direnv itself so you need to manually trigger a reload.

* Sublime Text
    * [Direnv](https://packagecontrol.io/packages/Direnv) ([caveat](https://github.com/misuzu/direnv-subl#limitations))
* JetBrains IDEs
    * [Direnv integration](https://plugins.jetbrains.com/plugin/15285-direnv-integration) ([JetBrains feature request](https://youtrack.jetbrains.com/issue/IDEA-320397))
* Visual Studio Code
    * [direnv](https://marketplace.visualstudio.com/items?itemName=mkhl.direnv) ([caveat](https://github.com/direnv/direnv-vscode/issues/109))
* Vim
    * [direnv.vim](https://github.com/direnv/direnv.vim)

## Developing

Common recipes are provided as Just recipes. To list them, run:

```shell
just
```

### Building the Package

To do a full release build (runs static analysis + unit tests), run:

```shell
just build
```

If you want to use a specific Python binary such as Python 3.9, run:

```shell
just python-binary=python3.9 build
```

### Running Tests

The project includes unit, integration, and end-to-end (E2E) tests. In most cases, you'll only run the unit and integration tests.

#### Unit Tests

Unit tests verify the functionality of individual components:

```shell
poetry run pytest tests/unit/
```

#### Integration Tests

Integration tests verify interactions between components and local storage services:

```shell
just start-storage-systems

just run-integration-tests

just stop-storage-systems
```

If you want to use a specific Python binary such as Python 3.9, run:

```shell
just python-binary=python3.9 run-integration-tests
```

## Notes

### Updating Flake Locks

The `flake.lock` file locks the inputs (e.g. the Nixpkgs revision) used to evaluate `flake.nix` files. To update the inputs (e.g. to get newer packages in a later Nixpkgs revision), you'll need to update your `flake.lock` file.

```shell
# Update flake.lock.
nix flake update
```
