"""Examples: python -m recommendation --bundle ... preferences --request '{...}'"""
import argparse
import json

from .engine import RecommendationEngine


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--bundle', required=True)
    sub = p.add_subparsers(dest='method', required=True)
    similar = sub.add_parser('similar')
    similar.add_argument('--listing-key', required=True)
    similar.add_argument('--limit', type=int, default=10)
    similar.add_argument('--as-of')
    pref = sub.add_parser('preferences')
    pref.add_argument('--request', required=True, help='JSON object with explicit sale/rent')
    args = p.parse_args()
    engine = RecommendationEngine(args.bundle)
    if args.method == 'similar':
        rows = engine.similar(args.listing_key, limit=args.limit, as_of=args.as_of)
    else:
        rows = engine.preferences(**json.loads(args.request))
    print(json.dumps({'items': rows}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
