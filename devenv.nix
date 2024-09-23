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

  # https://devenv.sh/packages/
  packages = with pkgs; [
    nixfmt-rfc-style
    bat
    jq
    tealdeer
    docker
    docker-compose
    go-task

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
      docker compose -f tests/docker-compose.yml up --build
    '';
  };

  enterShell = ''
    hello
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
