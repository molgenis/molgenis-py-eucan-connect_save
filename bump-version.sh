# get the last version from the tags
current_version="$(git tag | sort -r --version-sort | head -n1)"

# if there is no version, use 0.0.0
if [ -z "$current_version" ]
then
  current_version="0.0.0"
fi

# use bumpversion to bump the version based on the RELEASE_SCOPE and create a new tag
new_version="$(bumpversion --current-version "$current_version" --list "$RELEASE_SCOPE" | grep new_version= | cut -d'=' -f2)"
echo "$new_version"
