# Contributing

Contributions to this project are made under the Developer Certificate of Origin.

<details>
<summary>
Developer Certificate of Origin
</summary>

```text
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.


Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

</details>

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
│   ├── integ/
│   │   └── ...
│   │
│   │   # End-to-end (E2E) tests.
│   └── e2e/
│       └── ...
│
│   # GitLab pipeline entrypoint.
├── .gitlab-ci.yml
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

It also has [editor integration](https://github.com/direnv/direnv/wiki#editor-integration). Note that some integrations won't automatically reload the environment after Nix flake changes unlike direnv itself so manual reloads may be needed.

### Containerization

Some local storage services are only vended as OCI containers. We use the Docker CLI so we'll need a container runtime accessible through the Docker daemon socket.

Most people will use Docker Engine as the container runtime. This comes with [Docker Desktop](https://www.docker.com/products/docker-desktop) ([🍻](https://formulae.brew.sh/cask/docker)) and [Rancher Desktop](https://rancherdesktop.io) ([🍻](https://formulae.brew.sh/cask/rancher)).

Alternatively you can use Podman as the container runtime. This comes with the [Podman CLI](https://podman.io) ([🍺](https://formulae.brew.sh/formula/podman), [Podman socket instructions](https://docs.podman.io/en/latest/markdown/podman-system-service.1.html)) and [Podman Desktop](https://podman-desktop.io) ([🍻](https://formulae.brew.sh/cask/podman-desktop)).

## Developing

Common recipes are provided as Just recipes. To list them, run:

```shell
just
```

### Building the Project

To do a full release build, run:

```shell
just build
```

If you want to use a specific Python binary such as Python 3.9, run:

```shell
just python-binary=python3.9 build
```

## Notes

### Updating Flake Locks

The `flake.lock` file locks the inputs (e.g. the Nixpkgs revision) used to evaluate `flake.nix` files. To update the inputs (e.g. to get newer packages in a later Nixpkgs revision), you'll need to update your `flake.lock` file.

```shell
# Update flake.lock.
nix flake update
```
