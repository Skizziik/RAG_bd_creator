function slugifyTitle(value, fallback = 'wiki') {
  const slug = String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .slice(0, 80);
  return slug || fallback;
}

function splitTextIntoChunks(text, baseId, limit = 1800) {
  const normalized = String(text || '').trim();
  if (!normalized) return [];
  if (normalized.length <= limit) {
    return [{ id: baseId, text: normalized, index: 1, total: 1 }];
  }

  const chunks = [];
  let remaining = normalized;
  let index = 1;

  while (remaining.length > 0) {
    let cutPoint = Math.min(limit, remaining.length);
    if (remaining.length > limit) {
      const sectionBreak = remaining.lastIndexOf('\n## ', limit);
      const paragraphBreak = remaining.lastIndexOf('\n\n', limit);
      const sentenceBreak = remaining.lastIndexOf('. ', limit);

      if (sectionBreak > limit * 0.45) cutPoint = sectionBreak;
      else if (paragraphBreak > limit * 0.35) cutPoint = paragraphBreak;
      else if (sentenceBreak > limit * 0.35) cutPoint = sentenceBreak + 1;
    }

    const chunkText = remaining.slice(0, cutPoint).trim();
    if (chunkText) {
      chunks.push({
        id: `${baseId}_${index}`,
        text: chunkText,
        index,
      });
      index++;
    }
    remaining = remaining.slice(cutPoint).trim();
  }

  return chunks.map(chunk => ({ ...chunk, total: chunks.length }));
}

module.exports = {
  slugifyTitle,
  splitTextIntoChunks,
};