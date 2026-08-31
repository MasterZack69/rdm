{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    systems.url = "github:nix-systems/default-linux";
  };
  outputs =
    { self, nixpkgs, ... }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      eachSystem = nixpkgs.lib.genAttrs systems;
    in
    {
      packages = eachSystem (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.rustPlatform.buildRustPackage {
            pname = "rdm";
            version = "0.2.11";
            src = ./.;
            cargoLock.lockFile = ./Cargo.lock;
            doCheck = false;
            meta = {
              description = "Rust Download Manager - CLI";
              homepage = "https://github.com/MasterZack69/rdm";
              license = pkgs.lib.licenses.agpl3Only;
              platforms = pkgs.lib.platforms.linux;
              mainProgram = "rdm";
            };
          };
        }
      );
      checks = eachSystem (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = self.packages.${system}.default;
        }
      );
      apps = eachSystem (system: {
        default = {
          type = "app";
          program = "${self.packages.${system}.default}/bin/rdm";
        };
      });
      formatter = eachSystem (system: nixpkgs.legacyPackages.${system}.nixpkgs-fmt);
      devShells = eachSystem (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell { buildInputs = with pkgs; [ rustup ]; };
        }
      );
    };
}
