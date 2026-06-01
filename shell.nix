{
  pkgs ? import <nixpkgs> { },
}:

pkgs.mkShell {
  packages = with pkgs; [
    rustup
  ];

  shellHook = ''
    echo "Done"
    trap 'echo "Done"' EXIT
  '';
}
