{
  pkgs,
  lib,
  config,
  inputs,
  ...
}:

let
  python-packages =
    p: with p; [
      pip
      python-lsp-server
      epc
      black
      pylint
    ];
  compose-path = "./tests/docker-compose.yml";
in
{
  name = "sparkle";
  # https://devenv.sh/basics/
  env = {
    GREET = "🛠️ Let's hack ";
  };

  # https://devenv.sh/scripts/
  scripts.hello.exec = "echo $GREET";
  scripts.cat.exec = ''
    bat "$@";
  '';

  # This script is temporary due to two problems:
  #  1. `cz` requires a personal github token to publish a release https://commitizen-tools.github.io/commitizen/tutorials/github_actions/
  #  2. `cz bump` fails to sign in a terminal: https://github.com/commitizen-tools/commitizen/issues/1184
  scripts.release = {
    exec = ''
      rm CHANGELOG.md
      cz bump --files-only --check-consistency
      git tag $(python -c "from src.sparkle import __version__; print(__version__)")
    '';
    description = ''
      Release a new version and update the CHANGELOG.
    '';
  };

  # convenient shortcuts
  scripts.up.exec = "devenv up -d";
  scripts.up.description = "Start processes in the background.";

  scripts.down.exec = "devenv processes down";
  scripts.down.description = "Stop processes.";

  scripts.cleanup.exec = "docker compose -f ${compose-path} rm -vf";
  scripts.cleanup.description = "Remove unused docker containers and volumes.";

  scripts.show.exec = ''
    GREEN="\033[0;32m";
    YELLOW="\033[33m";
    NC="\033[0m";
    echo
    echo -e "✨ Helper scripts you can run to make your development richer:"
    echo

    ${pkgs.gnused}/bin/sed -e 's| |••|g' -e 's|=| |' <<EOF | ${pkgs.util-linuxMinimal}/bin/column -t | ${pkgs.gnused}/bin/sed -e "s|^\([^ ]*\)|$(printf "$GREEN")\1$(printf "$NC"):    |" -e "s|^|$(printf "$YELLOW*$NC") |" -e 's|••| |g'
    ${lib.generators.toKeyValue { } (
      lib.mapAttrs (name: value: value.description) (
        lib.filterAttrs (_: value: value.description != "") config.scripts
      )
    )}
    EOF

    echo
  '';
  scripts.show.description = "Print this message and exit.";

  # https://devenv.sh/packages/
  packages = with pkgs; [
    nixfmt-rfc-style
    bat
    jq
    tealdeer
    docker
    docker-compose

    # Python Dependencies
    (python3.withPackages python-packages)
  ];

  languages = {

    python = {
      enable = true;
      version = "3.10.14";
      poetry = {
        enable = true;
        activate.enable = true;
        install.enable = true;
        install.allExtras = true;
        install.groups = [ "dev" ];
      };
    };
  };

  languages.java.enable = true;
  languages.java.jdk.package = pkgs.jdk8; # Java version running on AWS Glue

  processes = {
    kafka-test.exec = ''
      docker compose -f ${compose-path} up --build
    '';
  };

  enterShell = ''
    hello
    show
  '';

  # https://devenv.sh/pre-commit-hooks/
  pre-commit.hooks = {
    nixfmt-rfc-style = {
      enable = true;
      excludes = [ ".devenv.flake.nix" ];
    };
    yamllint = {
      enable = true;
      settings.preset = "relaxed";
    };

    ruff.enable = true;
    editorconfig-checker.enable = true;
  };

  # Make diffs fantastic
  difftastic.enable = true;

  # https://devenv.sh/integrations/dotenv/
  dotenv.enable = true;

  # https://devenv.sh/integrations/codespaces-devcontainer/
  devcontainer.enable = true;
}
