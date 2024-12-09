#
# Nix flake.
#
# https://nix.dev/manual/nix/stable/command-ref/new-cli/nix3-flake#flake-format
# https://wiki.nixos.org/wiki/Flakes#Flake_schema
#
{
  description = "Nix flake.";

  inputs = {
    # https://search.nixos.org/packages?channel=unstable
    nixpkgs = {
      type = "github";
      owner = "NixOS";
      repo = "nixpkgs";
      ref = "nixpkgs-unstable";
    };
    # Python 3.8 was removed from Nixpkgs since it's EOL.
    #
    # https://github.com/NixOS/nixpkgs/pull/285779
    nixpkgs-python38 = {
      type = "github";
      owner = "NixOS";
      repo = "nixpkgs";
      # Commit right before removal.
      rev = "75b569f0a1bf5a6686bf1a4eca234b75e18d67a3";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      nixpkgs-python38,
      ...
    }:
    let
      # Systems to provide outputs for.
      systems = [
        "aarch64-darwin"
        "aarch64-linux"
        "x86_64-darwin"
        "x86_64-linux"
      ];

      # A function that creates an attribute set and provides a system-specific nixpkgs for each system.
      forEachSystem =
        f:
        nixpkgs.lib.genAttrs systems (
          system:
          f {
            pkgs = import nixpkgs {
              inherit system;
            };

            pkgs-python38 = import nixpkgs-python38 {
              inherit system;
            };
          }
        );
    in
    {
      # Formatter.
      #
      # For `nix fmt`.
      formatter = forEachSystem ({ pkgs, ... }: pkgs.nixfmt-rfc-style);

      # Development shells.
      #
      # For `nix develop` and direnv's `use flake`.
      devShells = forEachSystem (
        { pkgs, pkgs-python38, ... }:
        {
          default = pkgs.mkShell {
            packages =
              (with pkgs; [
                # Nix.
                #
                # Nix is dynamically linked on some systems. If we set LD_LIBRARY_PATH,
                # running Nix commands with the system-installed Nix may fail due to mismatched library versions.
                nix
                # Utilities.
                coreutils
                curl
                lsof
                netcat-gnu
                # Git.
                git
                git-lfs
                # Just.
                just
                # Python.
                python313
                python312
                python311
                python310
                python39
                # Poetry.
                poetry
                # Ruff.
                ruff
                # Pyright.
                pyright
                # Storage systems.
                azurite
                minio
                # Docker CLI.
                docker
                # JFrog CLI.
                jfrog-cli
                # OpenSSH.
                openssh
                # GitHub CLI.
                gh
              ])
              ++ (with pkgs-python38; [
                # Python.
                python38
              ]);

            shellHook = ''
              # Dynamic linker.
              #
              # See:
              # * https://discourse.nixos.org/t/how-to-solve-libstdc-not-found-in-shell-nix/25458
              # * https://discourse.nixos.org/t/poetry-pandas-issue-libz-so-1-not-found/17167
              export LD_LIBRARY_PATH=${
                with pkgs;
                lib.makeLibraryPath [
                  stdenv.cc.cc
                  zlib
                ]
              }

              echo "⚗️"
            '';
          };
        }
      );
    };
}
