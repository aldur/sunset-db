{
  description = "SunsetDB";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-23.05";
    nixpkgs-unstable.url = "github:NixOS/nixpkgs/nixos-unstable";

    crane = {
      url = "github:ipetkov/crane";
      inputs = {
        nixpkgs.follows = "nixpkgs";
        flake-utils.follows = "flake-utils";
        rust-overlay.follows = "rust-overlay";
      };
    };

    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.flake-utils.follows = "flake-utils";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";

    flake-utils.url = "github:numtide/flake-utils";

    advisory-db = {
      url = "github:rustsec/advisory-db";
      flake = false;
    };
  };

  outputs = { self, nixpkgs, nixpkgs-unstable, crane, rust-overlay, flake-utils, advisory-db, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlay-unstable = final: prev: {
          unstable = nixpkgs-unstable.legacyPackages.${prev.system};
        };

        overlays = [ (import rust-overlay) overlay-unstable ];

        pkgs = import nixpkgs {
          inherit system overlays;
        };

        inherit (pkgs) lib;

        # tell crane to use our toolchain
        rustToolchain = pkgs.pkgsBuildHost.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        unfilteredSrc = craneLib.path ./.;
        src = craneLib.cleanCargoSource unfilteredSrc;

        # Common arguments can be set here to avoid repeating them later
        commonArgs = {
          inherit src;

          buildInputs = [
            # Add additional build inputs here
          ] ++ lib.optionals pkgs.stdenv.isDarwin [
            # Additional darwin specific inputs can be set here
          ];

          # Additional environment variables can be set directly
          # MY_CUSTOM_VAR = "some value";
        };


        # Build *just* the cargo dependencies, so we can reuse
        # all of that work (e.g. via cachix) when running in CI
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        # Build the actual crate itself, reusing the dependency
        # artifacts from above.
        sunsetdb = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
        });
      in
      {
        checks = {
          # Build the crate as part of `nix flake check` for convenience
          inherit sunsetdb;

          # Run clippy (and deny all warnings) on the crate source,
          # again, resuing the dependency artifacts from above.
          #
          # Note that this is done as a separate derivation so that
          # we can block the CI if there are issues here, but not
          # prevent downstream consumers from building our crate by itself.
          sunsetdb-clippy = craneLib.cargoClippy (commonArgs // {
            inherit cargoArtifacts;
            cargoClippyExtraArgs = "--all-targets --all-features -- --deny warnings";
          });

          sunsetdb-doc = craneLib.cargoDoc (commonArgs // {
            inherit cargoArtifacts;
          });

          # Check formatting
          sunsetdb-fmt = craneLib.cargoFmt {
            inherit src;
          };

          # Audit dependencies
          sunsetdb-audit = craneLib.cargoAudit {
            inherit src advisory-db;
          };

          # TODO: Point to `advisory-db`
          sunsetdb-deny = craneLib.mkCargoDerivation {
            buildPhaseCargoCommand = ''
              cargo deny \
              --frozen --locked --offline \
              --all-features --color never \
              check licenses --disable-fetch
            '';

            cargoArtifacts = null; # Don't need artifacts, just Cargo.lock
            doInstallCargoArtifacts = false; # We don't expect to/need to install artifacts

            # Need cargo-deny >= 0.14 because of this:
            # https://github.com/EmbarkStudios/cargo-deny/pull/520
            # Otherwise, won't work w/ Nix derivation and will ALWAYS pull deps.
            nativeBuildInputs = [ pkgs.unstable.cargo-deny ];

            src = lib.cleanSourceWith {
              src = unfilteredSrc;
              filter = path: type: type == "directory"
                || lib.hasSuffix ".toml" path  # includes `deny*.toml` and `Cargo.toml`
                || lib.hasSuffix "Cargo.lock" path
                || lib.hasSuffix "LICENSE" path
                || (craneLib.filterCargoSources path type);
            };

            pnameSuffix = "-deny";
          };

          # Run tests with cargo-nextest
          # Consider setting `doCheck = false` on `sunsetdb` if you do not want
          # the tests to run twice
          sunsetdb-nextest = craneLib.cargoNextest (commonArgs // {
            inherit cargoArtifacts;
            partitions = 1;
            partitionType = "count";
          });
        } // lib.optionalAttrs (system == "x86_64-linux") {
          # NB: cargo-tarpaulin only supports x86_64 systems
          # Check code coverage (note: this will not upload coverage anywhere)
          sunsetdb-coverage = craneLib.cargoTarpaulin (commonArgs // {
            inherit cargoArtifacts;
          });
        };

        packages = {
          default = sunsetdb;
          # sunsetdb-llvm-coverage = craneLibLLvmTools.cargoLlvmCov (commonArgs // {
          #   inherit cargoArtifacts;
          # });
        };

        apps.default = flake-utils.lib.mkApp {
          drv = sunsetdb;
        };

        devShells.default = craneLib.devShell {
          # Inherit inputs from checks.
          checks = self.checks.${system};

          # Additional dev-shell environment variables can be set directly
          # MY_CUSTOM_DEVELOPMENT_VAR = "something else";

          # Extra inputs can be added here; cargo and rustc are provided by default.
          packages = [
            pkgs.neovim
            pkgs.ripgrep
            pkgs.fzf
            pkgs.git
          ];
        };
      });
}
