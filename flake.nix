{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    systems.url = "github:nix-systems/default";
  };

  outputs =
    { self, nixpkgs, systems, ... }:
    let
      eachSystem = nixpkgs.lib.genAttrs (import systems);
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
            src = ./.;

            cargoLock.lockFile = ./Cargo.lock;

            doCheck = false;

            meta = {
              description = "Rust Download Manager - CLI";
              homepage = "https://github.com/MasterZack69/rdm";
              license = pkgs.lib.licenses.agpl3Only;
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

      apps = eachSystem (
        system:
        {
          default = {
            type = "app";
            program = "${self.packages.${system}.default}/bin/rdm";
          };
        }
      );

      formatter = eachSystem (system: nixpkgs.legacyPackages.${system}.nixpkgs-fmt);

      devShells = eachSystem (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            buildInputs = with pkgs; [
              rustup
            ];
          };
        }
      );
    };
}
