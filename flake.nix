{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    systems.url = "github:nix-systems/default";
  };

  outputs =
    { nixpkgs, systems, ... }:
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
            version = "0.2.2";
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
