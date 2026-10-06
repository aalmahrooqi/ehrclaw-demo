#!/usr/bin/env python3
"""Read-only RxNav/openFDA lookups; curl owns network access, Python parses JSON."""
import argparse
import json
import re
import subprocess
import time
from urllib.parse import urlencode

RXNAV = 'https://rxnav.nlm.nih.gov/REST'
FDA = 'https://api.fda.gov/drug/label.json'
FIELDS = ('drug_interactions', 'boxed_warning', 'contraindications',
          'warnings_and_cautions', 'warnings', 'precautions', 'do_not_use',
          'ask_doctor', 'ask_doctor_or_pharmacist')
MAX_DRUGS = 3
MAX_REQUESTS = 36
MAX_SECONDS = 60
LIMIT = 5
UUID = re.compile(r'^[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$')


class LookupFailure(Exception):
    pass


def strings(value):
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, list):
        return [v for v in value if isinstance(v, str) and v.strip()]
    return []


def normalize(value):
    return ' '.join(re.findall(r'[a-z0-9]+', value.casefold()))


class Client:
    def __init__(self):
        self.deadline = time.monotonic() + MAX_SECONDS
        self.requests = []
        self.cache = {}

    def get(self, url, params=None):
        params = params or {}
        full_url = url + ('?' + urlencode(params) if params else '')
        if full_url in self.cache:
            return self.cache[full_url]
        # Two attempts total for transient failures; never retry permission errors.
        for attempt in range(2):
            remaining = self.deadline - time.monotonic()
            if remaining <= 0 or len(self.requests) >= MAX_REQUESTS:
                raise LookupFailure('Lookup budget exhausted; no further requests attempted.')
            timeout = min(8, remaining)
            args = ['curl', '-q', '--silent', '--show-error', '--proto', '=https',
                    '--connect-timeout', '3', '--max-time', str(timeout),
                    '--max-filesize', '4194304', '--get', '--header', 'Accept: application/json',
                    '--write-out', '\n%{http_code}', '--url', url]
            for key, value in params.items():
                args += ['--data-urlencode', f'{key}={value}']
            entry = {'url': full_url, 'attempt': attempt + 1}
            self.requests.append(entry)
            try:
                proc = subprocess.run(args, capture_output=True, text=True, timeout=timeout + 1)
            except (OSError, subprocess.TimeoutExpired) as error:
                raise LookupFailure('curl could not complete the lookup: ' + type(error).__name__) from error
            body, _, code = proc.stdout.rpartition('\n')
            status = int(code) if code.isdigit() else 0
            entry['http_status'] = status
            if proc.returncode or status in (408, 429, 500, 502, 503, 504):
                transient = proc.returncode in (0, 5, 6, 7, 28, 52, 56)
                if attempt == 0 and transient:
                    continue
                raise LookupFailure(f'Lookup failed (HTTP {status}, curl {proc.returncode}).')
            try:
                data = json.loads(body)
            except (ValueError, TypeError) as error:
                raise LookupFailure('The source returned invalid JSON.') from error
            if not isinstance(data, dict):
                raise LookupFailure('The source returned an unexpected JSON structure.')
            # Only an explicit FDA no-match response permits a name fallback.
            if status == 404 and url == FDA and isinstance(data.get('error'), dict) and data['error'].get('code') == 'NOT_FOUND':
                data = {'results': [], 'meta': {'results': {'total': 0}}}
            elif status != 200 or data.get('error'):
                raise LookupFailure(f'The source rejected the lookup (HTTP {status}).')
            self.cache[full_url] = data
            return data
        raise LookupFailure('Lookup failed.')


def resolve(client, value, is_rxcui=False):
    if is_rxcui:
        rxcui = value
    else:
        data = client.get(RXNAV + '/rxcui.json', {'name': value, 'search': '0'})
        ids = strings((data.get('idGroup') or {}).get('rxnormId'))
        if len(ids) != 1:
            reason = 'Name did not resolve.' if not ids else 'Name is ambiguous; candidate RxCUIs: ' + ', '.join(ids)
            raise LookupFailure(reason + ' Clarify the medicine; no label selected.')
        rxcui = ids[0]
    if not re.fullmatch(r'\d{1,12}', rxcui):
        raise LookupFailure('Invalid RxCUI; no label selected.')
    props = client.get(RXNAV + f'/rxcui/{rxcui}/properties.json').get('properties') or {}
    if props.get('rxcui') != rxcui or not props.get('name') or not props.get('tty'):
        raise LookupFailure('RxNorm did not verify an active concept for this RxCUI.')
    return props


def ingredient_aliases(client, concept):
    groups = (client.get(RXNAV + f'/rxcui/{concept["rxcui"]}/related.json',
                         {'tty': 'IN PIN'}).get('relatedGroup') or {}).get('conceptGroup') or []
    ingredients, precise = {}, {}
    for group in groups:
        target = ingredients if group.get('tty') == 'IN' else precise if group.get('tty') == 'PIN' else None
        if target is not None:
            for row in group.get('conceptProperties') or []:
                if row.get('rxcui') and row.get('name'):
                    target[row['rxcui']] = row['name']
    if concept['tty'] == 'IN':
        ingredients[concept['rxcui']] = concept['name']
    if concept['tty'] == 'PIN':
        precise[concept['rxcui']] = concept['name']
    # A single ingredient may have salt names; combinations must never be silently broadened.
    if len(ingredients) != 1:
        raise LookupFailure('No exact-product label, and a single ingredient could not be verified. No broader label selected.')
    names = sorted(set(ingredients.values()) | set(precise.values()))
    if len(names) > 8:
        raise LookupFailure('Too many ingredient variants for a bounded fallback; clarify the product.')
    return names


def label_matches(row, rxcui, aliases=None):
    meta = row.get('openfda') or {}
    if aliases is None:
        return rxcui in strings(meta.get('rxcui'))
    allowed = {normalize(v) for v in aliases}
    names = strings(meta.get('generic_name'))
    substances = strings(meta.get('substance_name'))
    # Reject partial-name matches and combination products, even if one substance matches.
    return bool(names) and all(normalize(v) in allowed for v in names + substances)


def usable_reference(row):
    return bool(UUID.fullmatch(str(row.get('id', ''))) and UUID.fullmatch(str(row.get('set_id', ''))))


def field_excerpt(value, terms):
    text = ' '.join(' '.join(strings(value)).split())
    if not text:
        return None
    matches = []
    if terms:
        pattern = re.compile('|'.join(re.escape(term) for term in terms), re.I)
        matches = list(pattern.finditer(text))
    if matches:
        windows = []
        for match in matches:
            start, end = max(0, match.start() - 180), min(len(text), match.end() + 360)
            if windows and start <= windows[-1][1]:
                windows[-1][1] = max(windows[-1][1], end)
            else:
                windows.append([start, end])
        pieces, remaining = [], 1600
        for start, end in windows:
            if remaining <= 0:
                break
            end = min(end, start + remaining)
            pieces.append({'start': start, 'end': end, 'text': text[start:end]})
            remaining -= end - start
    else:
        pieces = [{'start': 0, 'end': min(1600, len(text)), 'text': text[:1600]}]
    covered = sum(p['end'] - p['start'] for p in pieces)
    return {'excerpts': pieces, 'total_characters': len(text), 'truncated': covered < len(text),
            'literal_term_matches': len(matches) if terms else None}


def extract_label(row, terms):
    meta = row.get('openfda') or {}
    fields = {key: result for key in FIELDS if (result := field_excerpt(row.get(key), terms))}
    return {
        'id': row['id'], 'set_id': row['set_id'], 'effective_time': row.get('effective_time'),
        'brand_names': strings(meta.get('brand_name')), 'generic_names': strings(meta.get('generic_name')),
        'manufacturer_names': strings(meta.get('manufacturer_name')), 'rxcuis': strings(meta.get('rxcui')),
        'product_types': strings(meta.get('product_type')), 'routes': strings(meta.get('route')),
        'source_url': FDA + '?' + urlencode({'search': 'id:' + json.dumps(row['id']), 'limit': 1}),
        'label_url': 'https://dailymed.nlm.nih.gov/dailymed/drugInfo.cfm?setid=' + row['set_id'],
        'sections': fields,
    }


def lookup(client, value, is_rxcui=False, terms=()):
    first_request = len(client.requests)
    result = {'input': value, 'input_type': 'rxcui' if is_rxcui else 'name', 'status': 'Insufficient data'}
    try:
        concept = resolve(client, value, is_rxcui)
        rxcui = concept['rxcui']
        result.update(rxcui=rxcui, resolved_name=concept['name'], concept_type=concept['tty'])
        params = {'search': 'openfda.rxcui:' + json.dumps(rxcui), 'limit': LIMIT, 'sort': 'effective_time:desc'}
        data = client.get(FDA, params)
        rows = data.get('results')
        if not isinstance(rows, list):
            raise LookupFailure('Label response omitted its results array.')
        aliases = None
        basis = 'exact_rxcui'
        if not rows:
            aliases = ingredient_aliases(client, concept)
            params['search'] = '(' + ' OR '.join('openfda.generic_name:' + json.dumps(name) for name in aliases) + ')'
            data = client.get(FDA, params)
            rows = data.get('results')
            if not isinstance(rows, list):
                raise LookupFailure('Fallback response omitted its results array.')
            basis = 'ingredient_label_not_confirmed_patient_product'
            result['verified_ingredient_names'] = aliases
        matches = [r for r in rows if isinstance(r, dict) and usable_reference(r) and label_matches(r, rxcui, aliases)]
        result.update(match_basis=basis, candidates_checked=len(rows), matching_candidates=len(matches),
                      total_source_matches=(data.get('meta') or {}).get('results', {}).get('total'))
        if not matches:
            raise LookupFailure('No matching, identifiable label in the returned candidates. No interaction claim verified.')
        # Prefer an explicit interaction section, then the latest label among the bounded candidates.
        matches.sort(key=lambda r: (bool(strings(r.get('drug_interactions'))), str(r.get('effective_time', '')), r['id']), reverse=True)
        result['label'] = extract_label(matches[0], terms)
        result['status'] = 'Interaction label text found' if result['label']['sections'] else 'No interaction label text found'
        result['limitations'] = [
            f'One label selected from at most {LIMIT} returned candidates; this is not an exhaustive interaction check.',
            'Excerpts may be truncated. No mention is not evidence of safety.',
            'A charted prescription does not confirm actual use; no patient-specific safety decision was made.',
        ]
        if aliases is not None:
            result['limitations'].append('Ingredient fallback: dose, formulation, manufacturer, and the patient\'s actual product were not matched.')
    except LookupFailure as error:
        result['reason'] = str(error)
    except (AttributeError, TypeError, KeyError):
        result['status'] = 'Insufficient data'
        result.pop('label', None)
        result['reason'] = 'The source returned an unexpected JSON structure; no label evidence verified.'
    result['requests'] = client.requests[first_request:]
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--drug', action='append', default=[], help='Exact drug name; repeat for up to three medicines.')
    parser.add_argument('--rxcui', action='append', default=[], help='Known RxCUI from a source; repeat instead of --drug when available.')
    parser.add_argument('--term', action='append', default=[], help='Optional literal term to focus excerpts; repeat up to six times.')
    args = parser.parse_args()
    inputs = [(name.strip(), False) for name in args.drug] + [(code.strip(), True) for code in args.rxcui]
    if not 1 <= len(inputs) <= MAX_DRUGS:
        parser.error('Supply one to three medicines with --drug or --rxcui.')
    if any(not value or len(value) > 160 or any(ord(c) < 32 for c in value) for value, _ in inputs):
        parser.error('Medicine inputs must be nonempty, single-line strings of at most 160 characters.')
    if any(not re.fullmatch(r'\d{1,12}', value) for value, is_id in inputs if is_id):
        parser.error('RxCUIs must contain only digits.')
    if len(args.term) > 6 or any(not t.strip() or len(t) > 60 or any(ord(c) < 32 for c in t) for t in args.term):
        parser.error('Supply at most six nonempty single-line terms, each at most 60 characters.')
    client = Client()
    results = [lookup(client, value, is_id, args.term) for value, is_id in inputs]
    print(json.dumps({'schema_version': 1, 'results': results}, ensure_ascii=False, indent=2))
    return 0 if all(r['status'] != 'Insufficient data' for r in results) else 2


if __name__ == '__main__':
    raise SystemExit(main())
