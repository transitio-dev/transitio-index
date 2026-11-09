# checks.sh — what the rebuild scripts' checks share. Sourced, not run.

# Character classes spelled out: a range such as a-z can also match capitals,
# depending on the locale.
LOWER=abcdefghijklmnopqrstuvwxyz
UPPER=ABCDEFGHIJKLMNOPQRSTUVWXYZ
DIGITS=0123456789
HEX=0123456789abcdef

# An absolute path in plain form: printable ASCII only, no trailing slash, no
# empty, . or .. component, not / itself. A subshell, so LC_ALL=C stays here.
plain_path() (
  LC_ALL=C
  case "$1" in
    *[!\ -~]*|/*/|*//*|*/.|*/./*|*/..|*/../*|/) exit 1;;
    /?*) exit 0;;
    *) exit 1;;
  esac
)

# A build cache folder of the checkout: cache, or a slot's cache/slot-<n>.
cache_folder() {
  case "$1" in
    cache|cache/slot-[123456789]) return 0;;
    *) return 1;;
  esac
}
