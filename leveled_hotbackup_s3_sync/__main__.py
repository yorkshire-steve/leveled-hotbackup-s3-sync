import sys

from leveled_hotbackup_s3_sync.app import main

if __name__ == "__main__":
    RC = 1
    try:
        main()
        RC = 0
    except Exception as err:  # pylint: disable=broad-exception-caught
        print(f"Error: {err}", file=sys.stderr)
    sys.exit(RC)
