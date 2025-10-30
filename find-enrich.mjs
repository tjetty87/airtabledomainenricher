#!/usr/bin/env node
// find-enrich.mjs
// On each run, selects a small batch of Airtable rows that need enrichment,
// finds & verifies a domain, scrapes public contact email/phone, derives Industry
// from UK SIC (section-level), sets Enrichment Status, and stamps Last Enriched At.

import Airtable from 'airtable';
import axios from 'axios';
import { load } from 'cheerio';
import { Resolver } from 'dns';
import { promisify } from 'util';

// ---------- Env ----------
const {
  AIRTABLE_API_KEY,
  AIRTABLE_BASE_ID,
  AIRTABLE_TABLE_NAME,
  AIRTABLE_COMPANY_FIELD = 'Company',
  AIRTABLE_DOMAIN_FIELD = 'Domain',
  AIRTABLE_EMAIL_FIELD = 'Email',
  AIRTABLE_PHONE_FIELD = 'Phone',
  AIRTABLE_SIC_FIELD = 'SIC',                 // e.g. "62020" or "62020, 63110"
  AIRTABLE_INDUSTRY_FIELD = 'Industry',       // human-readable industry
  AIRTABLE_STATUS_FIELD = 'Enrichment Status',
  AIRTABLE_LAST_ENRICHED_AT_FIELD = 'Last Enriched At',
  AIRTABLE_COUNTRY_FIELD = '',                // optional; helps prefer .co.uk
  BATCH_SIZE = '5',
  DRY_RUN = 'false',

  // Optional: only process rows changed recently (in Airtable terms)
  // If set to a positive integer N, we only consider rows whose LAST_MODIFIED_TIME() (or CREATED_TIME)
  // is within the last N days.
  RECENT_DAYS = '0',
  USE_CREATED_TIME = 'false'                  // set "true" to use CREATED_TIME() instead of LAST_MODIFIED_TIME()
} = process.env;

if (!AIRTABLE_API_KEY || !AIRTABLE_BASE_ID || !AIRTABLE_TABLE_NAME) {
  console.error('❌ Missing env vars: AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_NAME');
  process.exit(1);
}

const batchSize = Math.max(1, parseInt(BATCH_SIZE, 10));
const filterRecentDays = Math.max(0, parseInt(RECENT_DAYS, 10));
const useCreatedTime = (USE_CREATED_TIME || 'false').toLowerCase() === 'true';

const base = new Airtable({ apiKey: AIRTABLE_API_KEY }).base(AIRTABLE_BASE_ID);
const table = base(AIRTABLE_TABLE_NAME);

// ---------- DNS/HTTP helpers ----------
const resolver = new Resolver();
resolver.setServers(['1.1.1.1', '8.8.8.8']);
const resolve4 = promisify(resolver.resolve4.bind(resolver));
const resolve6 = promisify(resolver.resolve6.bind(resolver));
const resolveMx = promisify(resolver.resolveMx.bind(resolver));

const sleep = (ms) => new Promise(r => setTimeout(r, ms));

async function httpAlive(domain) {
  const tries = [
    `https://${domain}`,
    `https://www.${domain}`,
    `http://${domain}`,
    `http://www.${domain}`,
  ];

  // First try HEAD (fast)
  for (const url of tries) {
    try {
      const res = await axios.head(url, { timeout: 4000, maxRedirects: 2, validateStatus: () => true });
      if (res.status > 0 && res.status < 600) return true;
    } catch { /* continue */ }
  }

  // Fallback: small GET (some hosts block HEAD)
  for (const url of tries) {
    try {
      const res = await axios.get(url, {
        timeout: 8000,
        maxRedirects: 2,
        validateStatus: () => true,
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; EnricherBot/1.0)', 'Accept': 'text/html,application/xhtml+xml' }
      });
      if (typeof res.data === 'string' && res.data.length > 100) return true;
      if (res.status >= 200 && res.status < 400) return true;
    } catch { /* continue */ }
  }
  return false;
}

async function dnsAlive(domain) {
  try {
    const [a, aaaa, mx] = await Promise.allSettled([resolve4(domain), resolve6(domain), resolveMx(domain)]);
    return [a, aaaa, mx].some(r => r.status === 'fulfilled' && r.value && r.value.length);
  } catch { return false; }
}

async function verify(domain) {
  const [dnsOk, httpOk] = await Promise.all([dnsAlive(domain), httpAlive(domain)]);
  return { domain, dnsOk, httpOk, ok: dnsOk || httpOk };
}

// ---------- Brand-matching helpers ----------
function tokenizeCompany(name) {
  const drop = new Set(['limited','ltd','plc','llp','inc','corp','corporation','company','group','holdings','services','uk','co','cic']);
  return (name || '')
    .toLowerCase()
    .replace(/[^\p{L}\p{N}\s-]/gu, ' ')
    .split(/\s+/)
    .filter(Boolean)
    .filter(t => !drop.has(t));
}

function brandMatchScore(company, htmlText) {
  if (!company || !htmlText) return 0;
  const tokens = tokenizeCompany(company);
  if (!tokens.length) return 0;
  const text = htmlText.toLowerCase();
  let hits = 0;
  for (const t of tokens) {
    if (t.length >= 3 && text.includes(t)) hits++;
  }
  return hits / tokens.length; // 0..1
}

// ---------- Domain candidate generation ----------
function cleanName(raw) {
  const drop = ['limited','ltd','plc','llp','inc','corp','corporation','company','tech','technologies','technology','solutions','solution','group','holdings','services','consulting','consultancy','studio','labs','lab','digital','global','international','uk','co'];
  let s = (raw || '').toLowerCase().replace(/[.,/\\'"&()\[\]{}!@#$%^*?+:=]+/g, ' ').replace(/\s+/g, ' ').trim();
  return s.split(' ').filter(t => !drop.includes(t)).join(' ');
}

function variants(name) {
  const v = new Set([name, name.replace(/\s+/g,''), name.replace(/\s+/g,'-')]);
  const parts = name.split(' ').filter(Boolean);
  const noVowels = name.replace(/[aeiou]/g,'').replace(/\s+/g,'');
  if (noVowels.length >= 5) v.add(noVowels);
  if (parts.length >= 2) {
    v.add(parts[0] + parts.at(-1));
    v.add(parts[0] + '-' + parts.at(-1));
    v.add(parts.slice(0,-1).map(p=>p[0]).join('') + parts.at(-1));
  }
  return Array.from(v);
}

function tldsForCountry(country) {
  const common = ['.com', '.co.uk', '.co', '.ai', '.io', '.net', '.org', '.uk'];
  if (!country) return common;
  const c = country.toLowerCase();
  if (['uk','united kingdom','england','scotland','wales','northern ireland'].includes(c)) {
    return ['.co.uk', '.uk', '.com', '.co', '.ai', '.io', '.net', '.org'];
  }
  return common;
}

function score(domain) {
  let s = 0;
  if (domain.endsWith('.co.uk')) s += 6;
  if (domain.endsWith('.com')) s += 5;
  if (domain.includes('-')) s -= 1;
  if (domain.length <= 12) s += 1;
  return s;
}

async function bestDomainFor(name, country) {
  const cleaned = cleanName(name);
  if (!cleaned) return { candidates: [], pick: null };

  const cands = [];
  for (const n of variants(cleaned)) for (const t of tldsForCountry(country)) cands.push(`${n}${t}`);
  cands.sort((a,b)=>score(b)-score(a));

  const TOP = Math.min(40, cands.length);
  const checked = [];
  for (let i = 0; i < TOP; i += 5) {
    const batch = cands.slice(i, i+5);
    const res = await Promise.all(batch.map(verify));
    checked.push(...res);
    await sleep(200);
    const strong = res.find(r => r.ok && (r.domain.endsWith('.co.uk') || r.domain.endsWith('.com')));
    if (strong) {
      const verified = checked.filter(r => r.ok).sort((a,b)=>score(b.domain)-score(a.domain));
      return { candidates: checked, pick: verified[0] || strong };
    }
  }
  const verified = checked.filter(r => r.ok).sort((a,b)=>score(b.domain)-score(a.domain));
  return { candidates: checked, pick: verified[0] || null };
}

// ---------- Contact extraction ----------
const EMAIL_RE = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
const PHONE_RE = /(?:(?:\+?44\s?\d{3,4}|\(?0\)?\s?\d{3,4})[\s-]?\d{3}[\s-]?\d{3,4}|\+?\d{1,3}[\s-]?\d{3,4}[\s-]?\d{3,4})/g;

const normalizePhone = (p) => {
  if (!p) return '';
  let s = p.replace(/[^+\d]/g, '');
  if (s.startsWith('0044')) s = '+' + s.slice(2);
  if (s.startsWith('44') && !s.startsWith('+')) s = '+' + s;
  return s;
};

function isLikelyPhone(n) {
  return /^\+?\d{10,15}$/.test(n); // 10..15 digits
}

function pickBestPhone(phones) {
  const arr = (phones || []).map(normalizePhone).filter(isLikelyPhone);
  if (!arr.length) return '';
  const scored = arr.map(n => {
    let s = 0;
    if (/^\+44\d{9,11}$/.test(n)) s += 2;   // UK bias
    if (n.length >= 11 && n.length <= 13) s += 0.5;
    return { n, s };
  }).sort((a,b)=>b.s-a.s);
  return scored[0].n;
}

function pickBestEmail(emails, preferredDomain = '') {
  const arr = (emails || []).map(e => e.trim()).filter(Boolean);
  if (!arr.length) return '';
  const freeDomains = ['gmail.com','yahoo.com','hotmail.com','outlook.com','live.com','aol.com','icloud.com','proton.me','protonmail.com'];
  const prefer = [];
  const others = [];
  for (const e of arr) {
    const d = e.split('@')[1]?.toLowerCase() || '';
    if (preferredDomain && d && (preferredDomain.endsWith(d) || d.endsWith(preferredDomain))) prefer.push(e);
    else others.push(e);
  }
  const pool = prefer.length ? prefer : others;
  const ranks = ['info@','contact@','hello@','support@','enquiries@'];
  const scored = pool.map(e => {
    const lower = e.toLowerCase();
    let s = 0;
    ranks.forEach((p,i)=>{ if (lower.startsWith(p)) s += (ranks.length - i); });
    if (!lower.includes('+')) s += 0.5;
    const d = lower.split('@')[1] || '';
    if (!freeDomains.includes(d)) s += 0.5;
    return { e, s };
  }).sort((a,b)=>b.s-a.s);
  return scored[0].e;
}

// Cloudflare email deobfuscation
function decodeCfEmail(hex) {
  try {
    const e = parseInt(hex.slice(0, 2), 16);
    let email = '';
    for (let n = 2; n < hex.length; n += 2) {
      const charCode = parseInt(hex.slice(n, n + 2), 16) ^ e;
      email += String.fromCharCode(charCode);
    }
    return email;
  } catch { return ''; }
}

async function getHtml(url) {
  try {
    const res = await axios.get(url, {
      timeout: 8000,
      maxRedirects: 3,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml',
        'Accept-Language': 'en-GB,en;q=0.8'
      }
    });
    return typeof res.data === 'string' ? res.data : '';
  } catch {
    return '';
  }
}

const makeAbs = (baseUrl, href) => { try { return new URL(href, baseUrl).toString(); } catch { return ''; } };

async function extractContactsFromUrl(url) {
  const html = await getHtml(url);
  if (!html) return { emails: [], phones: [] };
  const $ = load(html);

  const emails = new Set();
  const phones = new Set();

  // Standard mailto/tel
  $('a[href^="mailto:"]').each((_, a) => {
    const m = ($(a).attr('href') || '').replace(/^mailto:/i,'');
    if (m) emails.add(m.trim());
  });
  $('a[href^="tel:"]').each((_, a) => {
    const t = ($(a).attr('href') || '').replace(/^tel:/i,'');
    if (t) phones.add(t.trim());
  });

  // Cloudflare obfuscation
  $('[data-cfemail]').each((_, el) => {
    const hex = $(el).attr('data-cfemail') || '';
    const dec = decodeCfEmail(hex);
    if (dec) emails.add(dec.trim());
  });

  // Common data-* attributes
  $('[data-email]').each((_, el) => {
    const m = ($(el).attr('data-email') || '').trim();
    if (m) emails.add(m);
  });

  // Visible text scanning (last resort)
  const text = $.text();
  (text.match(EMAIL_RE) || []).forEach(e => emails.add(e));
  (text.match(PHONE_RE) || []).forEach(p => phones.add(p));

  return { emails: Array.from(emails), phones: Array.from(phones) };
}

async function discoverContacts(domain) {
  const baseUrl = `https://${domain}/`;
  const seeds = ['', 'contact', 'contact-us', 'contacts', 'about', 'about-us', 'team', 'imprint', 'legal', 'privacy']
    .map(p => makeAbs(baseUrl, p));

  const seen = new Set();
  const foundEmails = new Set();
  const foundPhones = new Set();

  for (const url of seeds) {
    if (!url || seen.has(url)) continue;
    seen.add(url);
    const { emails, phones } = await extractContactsFromUrl(url);
    emails.forEach(e => foundEmails.add(e));
    phones.forEach(p => foundPhones.add(p));
    await sleep(200);
  }

  // a few internal links from the homepage
  const homeHtml = await getHtml(baseUrl);
  if (homeHtml) {
    const $ = load(homeHtml);
    const internal = new Set();
    $('a[href]').each((_, a) => {
      const abs = makeAbs(baseUrl, $(a).attr('href') || '');
      if (abs && abs.startsWith(baseUrl)) internal.add(abs);
    });
    let count = 0;
    for (const u of internal) {
      if (count >= 3) break;
      if (seen.has(u)) continue;
      seen.add(u);
      const { emails, phones } = await extractContactsFromUrl(u);
      emails.forEach(e => foundEmails.add(e));
      phones.forEach(p => foundPhones.add(p));
      count++;
      await sleep(150);
    }
  }

  const brandText = (homeHtml || '').toString();
  return {
    email: pickBestEmail(Array.from(foundEmails), domain),
    phone: pickBestPhone(Array.from(foundPhones)),
    brandText
  };
}

// ---------- SIC → Industry (UK SIC 2007 section-level) ----------
const SIC_SECTIONS = [
  { range:[1,3],     name:'A — Agriculture, Forestry & Fishing' },
  { range:[5,9],     name:'B — Mining & Quarrying' },
  { range:[10,33],   name:'C — Manufacturing' },
  { range:[35,35],   name:'D — Electricity, Gas, Steam & Air Conditioning' },
  { range:[36,39],   name:'E — Water Supply; Sewerage, Waste Management & Remediation' },
  { range:[41,43],   name:'F — Construction' },
  { range:[45,47],   name:'G — Wholesale & Retail Trade; Repair of Motor Vehicles & Motorcycles' },
  { range:[49,53],   name:'H — Transportation & Storage' },
  { range:[55,56],   name:'I — Accommodation & Food Service Activities' },
  { range:[58,63],   name:'J — Information & Communication' },
  { range:[64,66],   name:'K — Financial & Insurance Activities' },
  { range:[68,68],   name:'L — Real Estate Activities' },
  { range:[69,75],   name:'M — Professional, Scientific & Technical Activities' },
  { range:[77,82],   name:'N — Administrative & Support Service Activities' },
  { range:[84,84],   name:'O — Public Administration & Defence; Compulsory Social Security' },
  { range:[85,85],   name:'P — Education' },
  { range:[86,88],   name:'Q — Human Health & Social Work Activities' },
  { range:[90,93],   name:'R — Arts, Entertainment & Recreation' },
  { range:[94,96],   name:'S — Other Service Activities' },
  { range:[97,98],   name:'T — Activities of Households as Employers; Undifferentiated Goods- & Services-Producing Activities of Households for Own Use' },
  { range:[99,99],   name:'U — Activities of Extraterritorial Organisations & Bodies' }
];

function lookupIndustryFromSICCode(sicCode) {
  if (!sicCode) return '';
  const two = parseInt(String(sicCode).trim().slice(0,2), 10);
  if (Number.isNaN(two)) return '';
  const match = SIC_SECTIONS.find(s => two >= s.range[0] && two <= s.range[1]);
  return match ? match.name : '';
}

function deriveIndustryFromSICField(sicFieldValue) {
  if (!sicFieldValue) return '';
  const values = Array.isArray(sicFieldValue) ? sicFieldValue : String(sicFieldValue).split(/[,;/\s]+/);
  const labels = new Set();
  for (const raw of values) {
    const label = lookupIndustryFromSICCode(raw);
    if (label) labels.add(label);
  }
  return Array.from(labels).join(' | ');
}

// ---------- Airtable selection ----------
function fieldBlank(field) { return `OR({${field}} = '', {${field}} = BLANK())`; }

function needEnrichmentFilter() {
  const needDomain = fieldBlank(AIRTABLE_DOMAIN_FIELD);
  const needEmail  = fieldBlank(AIRTABLE_EMAIL_FIELD);
  const needPhone  = fieldBlank(AIRTABLE_PHONE_FIELD);
  const needInd    = fieldBlank(AIRTABLE_INDUSTRY_FIELD);

  let f = `OR(${needDomain}, ${needEmail}, ${needPhone}, ${needInd})`;
  if (filterRecentDays > 0) {
    const timeFn = useCreatedTime ? 'CREATED_TIME()' : 'LAST_MODIFIED_TIME()';
    f = `AND(${f}, IS_AFTER(${timeFn}, DATEADD(TODAY(), -${filterRecentDays}, 'days')))`;
  }
  return f;
}

async function selectBatchNeedingEnrichment() {
  const page = await table.select({
   // maxRecords: batchSize,
  //  pageSize: batchSize,
    filterByFormula: needEnrichmentFilter(),
    sort: [{ field: AIRTABLE_COMPANY_FIELD, direction: 'asc' }],
  }).firstPage((err, records) => {
  console.log("total record length",records.length); // Max 100
});
  return page;
}

function getField(rec, field) {
  const v = rec.get(field);
  if (!v) return '';
  if (Array.isArray(v)) return v[0];
  return v;
}

// ---------- Main ----------
function nowIso() { return new Date().toISOString(); }

async function processRecord(rec) {
  const company  = getField(rec, AIRTABLE_COMPANY_FIELD);
  const country  = AIRTABLE_COUNTRY_FIELD ? getField(rec, AIRTABLE_COUNTRY_FIELD) : '';
  const sicRaw   = AIRTABLE_SIC_FIELD ? rec.get(AIRTABLE_SIC_FIELD) : '';
  let domain     = getField(rec, AIRTABLE_DOMAIN_FIELD);
  let email      = getField(rec, AIRTABLE_EMAIL_FIELD);
  let phone      = getField(rec, AIRTABLE_PHONE_FIELD);
  let industry   = getField(rec, AIRTABLE_INDUSTRY_FIELD);
  let statusNote = '';

  console.log(`• ${rec.id} — ${company}`);

  // 1) Domain
  if (!domain) {
    const { pick } = await bestDomainFor(company, country);
    if (pick) {
      domain = pick.domain;
      console.log(`  ˳ domain → ${domain}`);
    } else {
      console.log('  ˳ domain → (none found)');
    }
  }

  // 2) Contacts
  if (domain && (!email || !phone)) {
    console.log('  ˳ discovering contacts…');
    const c = await discoverContacts(domain);

    let bm = 0;
    if (typeof brandMatchScore === 'function') {
      bm = brandMatchScore(company, c.brandText || '');
    }
    const BRAND_OK = bm >= 0.4; // tune 0.3–0.5
    console.log(`  ˳ brand-match score: ${(bm*100).toFixed(0)}% ${BRAND_OK ? '(OK)' : '(weak)'}`);

    if (!email && c.email) { email = c.email; console.log(`  ˳ email  → ${email}`); }
    if (!phone && c.phone) { phone = c.phone; console.log(`  ˳ phone  → ${phone}`); }
    rec.__BRAND_OK = BRAND_OK;
  }

  // 3) Industry from SIC
  if (!industry && sicRaw) {
    const derived = deriveIndustryFromSICField(sicRaw);
    if (derived) { industry = derived; console.log(`  ˳ industry → ${industry}`); }
  }

  // Status
  if (!domain && !email && !phone) {
    statusNote = 'No domain or contacts found';
  } else if (domain && !email && !phone) {
    statusNote = 'Domain only';
  } else if (domain && (email || phone)) {
    const brandOk = rec.__BRAND_OK;
    statusNote = (typeof brandOk !== 'undefined' && brandOk === false)
      ? 'Domain only (unverified brand match)'
      : 'OK';
  } else {
    statusNote = 'Partial';
  }

  // Write back
  const patch = {};
  if (domain)   patch[AIRTABLE_DOMAIN_FIELD]           = domain;
  if (email)    patch[AIRTABLE_EMAIL_FIELD]            = email;
  if (phone)    patch[AIRTABLE_PHONE_FIELD]            = phone;
  if (industry) patch[AIRTABLE_INDUSTRY_FIELD]         = industry;
  patch[AIRTABLE_STATUS_FIELD]                         = statusNote;
  patch[AIRTABLE_LAST_ENRICHED_AT_FIELD]               = nowIso();

  if (DRY_RUN.toLowerCase() !== 'true') {
    console.log('rec.id:',rec.id);
    await table.update(rec.id, patch);
    console.log('  ✍️  updated:', patch);
  } else {
    console.log('  (DRY_RUN=true, not writing) would update:', patch);
  }
}

async function main() {
  const recs = await selectBatchNeedingEnrichment();
  if (!recs.length) {
    console.log('✅ No records need enrichment.');
    return;
  }
  for (const rec of recs) {
    try { await processRecord(rec); }
    catch (e) { console.error('  ⚠️ error:', e?.response?.data || e?.message || e); }
    await sleep(300);
  }
}

main().catch(e => {
  console.error('Fatal:', e?.response?.data || e);
  process.exit(1);
});
