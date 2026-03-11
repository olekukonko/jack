recursive = true
output_file = "all.txt"
extensions = [".go"]
exclude_dirs {
  items = ["_examples", "_lab", "_tmp"]
}
exclude_files {
  items =  ["LICENSE","README.md"]
}
use_gitignore = true
detailed = false