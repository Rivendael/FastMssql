#!/bin/bash

# check_direct_updates.sh: Checks a Rust project's *direct* dependencies for newer versions.

# --- Configuration ---
# The key change: --depth 1 limits the check to only direct dependencies
# --exit-code 1 is used to ensure the script fails if updates are found.
CARGO_OUTDATED_ARGS="--exit-code 1 --depth 1"
# If you want to include dev-dependencies as well, use:
# CARGO_OUTDATED_ARGS="--exit-code 1 --depth 1 --dev-dependencies"
# The default check includes [dependencies] and [build-dependencies].
# Adding --dev-dependencies will include your [dev-dependencies] section too.

# --- Main Logic ---

echo "🔎 Checking *direct* dependencies (depth 1) for updates using 'cargo outdated'..."
echo "---"

# Run cargo outdated with --depth 1 to filter out subdependencies.
cargo outdated $CARGO_OUTDATED_ARGS

# Capture the exit code of the last command (cargo outdated)
EXIT_CODE=$?

echo "---"

if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Success: All direct dependencies are up to date! (Exit Code: $EXIT_CODE)"
elif [ $EXIT_CODE -eq 1 ]; then
    echo "⚠️ Direct Updates Found: Please review the list above and consider updating your Cargo.toml."
    echo "    (Exit Code: $EXIT_CODE - intentional failure to signal updates)"
else
    echo "❌ Error: 'cargo outdated' failed with Exit Code: $EXIT_CODE"
    echo "    Check if 'cargo outdated' is installed and your 'Cargo.toml' is valid."
fi

# Exit with the same code as cargo outdated.
exit $EXIT_CODE