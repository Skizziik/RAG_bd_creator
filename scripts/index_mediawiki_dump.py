import argparse
import bz2
import json
import os
import re
import xml.etree.ElementTree as ET

CATEGORY_RE = re.compile(r'\[\[Category:([^|\]]+)', re.IGNORECASE)


def open_dump(path):
    if path.endswith('.bz2'):
        return bz2.open(path, 'rb')
    return open(path, 'rb')


def text_or_empty(element, tag, ns):
    child = element.find(f'{ns}{tag}')
    return child.text if child is not None and child.text else ''


def extract_categories(wikitext):
    return sorted({match.group(1).strip() for match in CATEGORY_RE.finditer(wikitext or '') if match.group(1).strip()})


def emit(event, **payload):
    print('EVENT ' + json.dumps({'event': event, **payload}), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True)
    parser.add_argument('--output-dir', required=True)
    parser.add_argument('--base-url', required=True)
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    pages_path = os.path.join(args.output_dir, 'pages.jsonl')
    categories_path = os.path.join(args.output_dir, 'categories.json')

    page_count = 0
    content_pages = 0
    redirect_pages = 0
    categories = {}
    ns = ''

    with open_dump(args.input) as source, open(pages_path, 'w', encoding='utf-8') as pages_file:
        context = ET.iterparse(source, events=('start', 'end'))
        _, root = next(context)
        if root.tag.startswith('{'):
            ns = root.tag.split('}')[0] + '}'

        for event, elem in context:
            if event != 'end' or elem.tag != f'{ns}page':
                continue

            title = text_or_empty(elem, 'title', ns).strip()
            namespace = int(text_or_empty(elem, 'ns', ns) or '0')
            redirect = elem.find(f'{ns}redirect') is not None
            revision = elem.find(f'{ns}revision')
            text = ''
            if revision is not None:
                text_node = revision.find(f'{ns}text')
                text = text_node.text if text_node is not None and text_node.text else ''

            page = {
                'title': title,
                'ns': namespace,
                'redirect': redirect,
                'wikitext': text,
                'categories': extract_categories(text),
                'source_url': f"{args.base_url.rstrip('/')}/wiki/{title.replace(' ', '_')}",
            }
            pages_file.write(json.dumps(page, ensure_ascii=False) + '\n')

            page_count += 1
            if namespace == 0 and not redirect:
                content_pages += 1
                for category in page['categories']:
                    categories[category] = categories.get(category, 0) + 1
            if redirect:
                redirect_pages += 1

            if page_count % 200 == 0:
                emit('progress', pages=page_count, content_pages=content_pages)

            elem.clear()
            root.clear()

    with open(categories_path, 'w', encoding='utf-8') as categories_file:
        json.dump({
            'categories': [{'name': name, 'count': count} for name, count in sorted(categories.items())],
            'pages': page_count,
            'content_pages': content_pages,
            'redirect_pages': redirect_pages,
        }, categories_file, ensure_ascii=False, indent=2)

    emit('complete', pages=page_count, content_pages=content_pages, redirect_pages=redirect_pages, categories=len(categories))


if __name__ == '__main__':
    main()