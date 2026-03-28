#!/usr/bin/env python3
"""
Deal Research Generator - Creates pre-filled Google Docs for sales deal research.

Usage: python deal_research.py "Company Name" "domain.com" ["Champion Name"]

This tool:
1. Fetches company data from Apollo API
2. Detects tech stack via web scraping
3. Searches for LinkedIn contacts via Tavily API + Gemini formatting
4. Gathers recent news and activity via Tavily API
5. Uses Gemini API to synthesize research sections
6. Creates a formatted Google Doc in the specified folder

Requirements:
- Apollo API key (required)
- Gemini API key (required)
- Tavily API key (optional - improves LinkedIn search quality and enables news section)
- Google OAuth credentials (required)
- Google Drive folder ID (required)
"""

import argparse
import json
import os
import re
import sys
import time
import uuid
import webbrowser
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent / ".env")

GOOGLE_WORKSPACE_PATH = Path(
    os.environ.get("GOOGLE_WORKSPACE_PATH", str(Path.home() / "Projects" / "google-workspace"))
).expanduser()
if GOOGLE_WORKSPACE_PATH.exists():
    sys.path.insert(0, str(GOOGLE_WORKSPACE_PATH))

import requests
from bs4 import BeautifulSoup
from google_workspace.auth import build_service
from llm_gateway import LLMGateway

# Tavily import - optional, will fail gracefully if not installed
try:
    from tavily import TavilyClient
    TAVILY_AVAILABLE = True
except ImportError:
    TAVILY_AVAILABLE = False

# =============================================================================
# CONFIGURATION
# =============================================================================

def load_config():
    """Load configuration from environment variables."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # dotenv not installed, rely on system environment variables

    # Support workflow aliases for gateway and provider credentials.
    alias_pairs = [
        ("GEMINI_API_KEY", "AI_GEMINI_KEY"),
        ("OPENAI_API_KEY", "AI_OPENAI_KEY"),
        ("DEEPSEEK_API_KEY", "AI_DEEPSEEK_KEY"),
    ]
    for canonical_name, alias_name in alias_pairs:
        if not os.environ.get(canonical_name) and os.environ.get(alias_name):
            os.environ[canonical_name] = os.environ[alias_name]

    required = ["APOLLO_API_KEY", "GOOGLE_DRIVE_FOLDER_ID"]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        print("Please copy .env.example to .env and fill in your API keys.")
        sys.exit(1)

    return {
        "apollo_api_key": os.environ["APOLLO_API_KEY"],
        "gemini_api_key": os.environ.get("GEMINI_API_KEY", ""),
        "tavily_api_key": os.environ.get("TAVILY_API_KEY"),
        "brave_api_key": os.environ.get("BRAVE_API_KEY"),
        "perplexity_api_key": os.environ.get("PERPLEXITY_API_KEY"),
        "openai_api_key": os.environ.get("OPENAI_API_KEY", ""),
        "deepseek_api_key": os.environ.get("DEEPSEEK_API_KEY", ""),
        "google_drive_folder_id": os.environ["GOOGLE_DRIVE_FOLDER_ID"],
    }

# Global config - loaded at runtime
CONFIG = None

def get_config():
    """Get or initialize configuration."""
    global CONFIG
    if CONFIG is None:
        CONFIG = load_config()
    return CONFIG


# Tech stack patterns to detect on websites
TECH_PATTERNS = {
    # CRM
    "Salesforce": [r"salesforce\.com", r"force\.com", r"lightning\.force"],
    "HubSpot CRM": [r"hubspot\.com", r"hs-scripts\.com", r"hbspt"],
    "Microsoft Dynamics": [r"dynamics\.com", r"crm\.dynamics"],
    # Marketing Automation
    "Marketo": [r"marketo\.net", r"marketo\.com", r"munchkin"],
    "Pardot": [r"pardot\.com", r"pi\.pardot"],
    "Eloqua": [r"eloqua\.com", r"elqcfg", r"elqtrack"],
    "HubSpot Marketing": [r"forms\.hubspot\.com", r"track\.hubspot"],
    # ABM & Intent
    "6sense": [r"6sense\.com", r"6sc\.co", r"j\.6sc\.co"],
    "Demandbase": [r"demandbase\.com", r"tag\.demandbase"],
    "Terminus": [r"terminus\.com", r"terminusplatform"],
    "RollWorks": [r"rollworks\.com"],
    "Bombora": [r"bombora\.com", r"ml314\.com"],
    # Sales Engagement
    "Outreach": [r"outreach\.io"],
    "Salesloft": [r"salesloft\.com"],
    "Apollo": [r"apollo\.io"],
    "Groove": [r"groove\.co"],
    # Conversational / Chat
    "Drift": [r"drift\.com", r"js\.driftt\.com"],
    "Intercom": [r"intercom\.io", r"intercomcdn\.com"],
    "Qualified": [r"qualified\.com"],
    "LiveChat": [r"livechat\.com", r"livechatinc\.com"],
    # CMS / Web
    "WordPress": [r"wp-content", r"wp-includes", r"wordpress"],
    "Drupal": [r"drupal\.js", r"drupal\.org"],
    "Contentful": [r"contentful\.com", r"ctfassets\.net"],
    "Webflow": [r"webflow\.com", r"assets-global\.website-files"],
    # Analytics & Attribution
    "Google Analytics": [r"google-analytics\.com", r"googletagmanager\.com", r"gtag"],
    "Adobe Analytics": [r"omniture\.com", r"2o7\.net", r"demdex\.net"],
    "Bizible": [r"bizible\.com", r"bizibly"],
    "Segment": [r"segment\.com", r"segment\.io", r"cdn\.segment"],
    "Heap": [r"heap\.io", r"heapanalytics\.com"],
    "Mixpanel": [r"mixpanel\.com"],
    # Event / Webinar
    "ON24": [r"on24\.com"],
    "Zoom": [r"zoom\.us"],
    "Cvent": [r"cvent\.com"],
    "Hopin": [r"hopin\.com"],
    # Other RevTech
    "ZoomInfo": [r"zoominfo\.com", r"ws\.zoominfo"],
    "Clearbit": [r"clearbit\.com", r"reveal\.clearbit"],
    "Gong": [r"gong\.io"],
    "Chorus": [r"chorus\.ai"],
}

# =============================================================================
# APOLLO API INTEGRATION
# =============================================================================

def fetch_apollo_data(domain):
    """Fetch company firmographics and tech stack from Apollo API."""
    print(f"  [Apollo] Fetching data for {domain}...")
    config = get_config()

    url = "https://api.apollo.io/api/v1/organizations/enrich"
    headers = {
        "Cache-Control": "no-cache",
        "Content-Type": "application/json",
        "X-Api-Key": config["apollo_api_key"],
    }
    params = {"domain": domain}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        org = data.get("organization", {})
        if not org:
            print("  [Apollo] No organization data found")
            return None

        # Extract tech stack from Apollo
        tech_stack = []
        for tech in org.get("current_technologies", []):
            if isinstance(tech, dict):
                tech_stack.append({
                    "name": tech.get("name", "Unknown"),
                    "category": tech.get("category", "Unknown")
                })
            else:
                tech_stack.append({"name": str(tech), "category": "Unknown"})

        result = {
            "name": org.get("name", "Unknown"),
            "domain": domain,
            "industry": org.get("industry", "Unknown"),
            "estimated_employees": org.get("estimated_num_employees"),
            "annual_revenue": org.get("annual_revenue_printed", "Unknown"),
            "annual_revenue_raw": org.get("annual_revenue"),
            "founded_year": org.get("founded_year"),
            "short_description": org.get("short_description", ""),
            "long_description": org.get("seo_description", ""),
            "city": org.get("city", ""),
            "state": org.get("state", ""),
            "country": org.get("country", ""),
            "linkedin_url": org.get("linkedin_url", ""),
            "twitter_url": org.get("twitter_url", ""),
            "facebook_url": org.get("facebook_url", ""),
            "total_funding": org.get("total_funding"),
            "total_funding_printed": org.get("total_funding_printed"),
            "latest_funding_round_type": org.get("latest_funding_round_type"),
            "latest_funding_round_date": org.get("latest_funding_round_date"),
            "latest_funding_round_amount": org.get("latest_funding_round_amount"),
            "keywords": org.get("keywords", []),
            "tech_stack": tech_stack,
            "raw_data": org,
        }

        print(f"  [Apollo] Found: {result['name']} - {result['industry']}")
        return result

    except requests.RequestException as e:
        print(f"  [Apollo] Error: {e}")
        return None


# =============================================================================
# WEB SCRAPING FOR TECH STACK
# =============================================================================

def scrape_website_tech_stack(domain):
    """Scrape website to detect marketing/sales tech stack."""
    print(f"  [Scraper] Scanning {domain} for tech stack...")

    url = f"https://{domain}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    try:
        response = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        response.raise_for_status()
        html = response.text
    except requests.RequestException as e:
        print(f"  [Scraper] Error fetching {url}: {e}")
        return []

    detected = []
    soup = BeautifulSoup(html, "html.parser")

    # Get all script sources and inline scripts
    script_srcs = []
    script_content = ""
    for script in soup.find_all("script"):
        if script.get("src"):
            script_srcs.append(script["src"])
        if script.string:
            script_content += script.string

    # Also check meta tags, link tags, and iframes
    for meta in soup.find_all("meta"):
        script_content += str(meta.get("content", ""))
    for link in soup.find_all("link"):
        script_srcs.append(str(link.get("href", "")))
    for iframe in soup.find_all("iframe"):
        script_srcs.append(str(iframe.get("src", "")))

    # Combine all searchable content
    searchable = html + "\n" + "\n".join(script_srcs) + "\n" + script_content

    # Check for each tech pattern
    for tech_name, patterns in TECH_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, searchable, re.IGNORECASE):
                if tech_name not in detected:
                    detected.append(tech_name)
                break

    print(f"  [Scraper] Detected {len(detected)} technologies")
    return sorted(detected)


# =============================================================================
# GEMINI DEEP RESEARCH FOR LINKEDIN CONTACTS
# =============================================================================

def search_linkedin_contacts_with_gemini(company_name):
    """Use Gemini with Google Search grounding to find LinkedIn contacts."""
    print(f"  [Gemini Deep Research] Searching for contacts at {company_name}...")

    prompt = f"""# ROLE
Act as an Executive Sales Researcher. Your goal is to identify high-value decision-makers at {company_name} by searching LinkedIn.

# TASK
Search LinkedIn to find specific individuals currently working at {company_name}. Find these three groups:

1. Corporate Leadership: CEO, CMO, CRO, CFO, COO, and Founders
2. Marketing Leadership: VPs and Directors in Marketing
3. Specialists: Anyone with "ABM", "Demand Generation", or "Digital Marketing" in their title

# SEARCH QUERIES TO EXECUTE
Search Google for:
site:linkedin.com/in "{company_name}" CEO
site:linkedin.com/in "{company_name}" CMO
site:linkedin.com/in "{company_name}" "VP Marketing"
site:linkedin.com/in "{company_name}" "Director Marketing"
site:linkedin.com/in "{company_name}" "Demand Generation"
site:linkedin.com/in "{company_name}" Founder

# CRITICAL REQUIREMENTS
1. You MUST include the actual LinkedIn profile URL for each person
2. Do NOT use placeholder text like "[Not available]" - if you cannot find the URL, omit that person
3. Only include people who CURRENTLY work at {company_name}
4. Aim to find 8-15 relevant contacts

# CRITICAL FORMATTING INSTRUCTIONS
This output will be pasted into Google Docs which does NOT render markdown.
DO NOT use any markdown syntax (no **, no *, no #, no [], no ()).

Use this EXACT plain text format for each contact:

CONTACT NAME
Title: [Their Current Title at {company_name}]
LinkedIn: [Full URL like https://www.linkedin.com/in/username]
Tenure: [Time at company if known]
Location: [City, State/Country]
Insight: [Brief note about their background]

(blank line between contacts)

EXAMPLE:

Jane Smith
Title: Chief Marketing Officer at Acme Corp
LinkedIn: https://www.linkedin.com/in/janesmith
Tenure: 3 years at company
Location: Boston, MA
Insight: Previously VP Marketing at HubSpot, strong background in ABM and demand generation

John Doe
Title: VP of Demand Generation at Acme Corp
LinkedIn: https://www.linkedin.com/in/johndoe
Tenure: 2 years at company
Location: San Francisco, CA
Insight: Built demand gen team from scratch, expertise in 6sense and Marketo

Now search and find the contacts at {company_name}:"""

    try:
        result = _call_gemini_grounded(prompt, max_tokens=8192)

        if result:
            contact_count = result.lower().count("linkedin.com/in/") + result.lower().count("linkedin profile")
            name_count = len([line for line in result.split('\n') if '**Name:**' in line or '**Title:**' in line])
            contact_estimate = max(contact_count, name_count // 2)
            print(f"  [Gemini Deep Research] Found ~{contact_estimate} contacts")
            return result

        return ""
    except Exception as e:
        print(f"  [Gemini Deep Research] Error: {e}")
        return ""


def search_champion_contact(champion_name, company_name):
    """
    Search for a specific champion contact on LinkedIn using Gemini grounded search.

    Args:
        champion_name: full name of the champion (e.g. "Rajiv Chidambaram")
        company_name: company they work at

    Returns:
        tuple: (champion_text, champion_url) or (None, None) if not found.
        champion_text is in the standard contact format (Name, Title, LinkedIn, etc.)
    """
    print(f"  [Champion] Searching for {champion_name} at {company_name}...")

    prompt = f"""Search LinkedIn for "{champion_name}" who works at "{company_name}".

Find their LinkedIn profile and provide the following information:

{champion_name}
Title: [their current title at {company_name}]
LinkedIn: [full LinkedIn profile URL like https://www.linkedin.com/in/username]
Tenure: [time at company if available]
Location: [city, state/country if available]
Insight: [brief background note from their profile]

Only return information if you find a LinkedIn profile that matches this person at {company_name}.
Do not use markdown formatting. Use plain text only."""

    try:
        result = _call_gemini_grounded(prompt, max_tokens=2048)
        if not result:
            print(f"  [Champion] Could not find {champion_name}")
            return None, None

        # Extract LinkedIn URL from result
        url_match = re.search(r'https?://(?:www\.)?linkedin\.com/in/[^\s,)]+', result)
        if not url_match:
            print(f"  [Champion] No LinkedIn URL found for {champion_name}")
            return None, None

        champion_url = url_match.group(0).rstrip('.')

        # Clean up the result to match expected format
        # Ensure it starts with the champion name and uses our format
        lines = result.strip().split('\n')
        cleaned_lines = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            # Skip markdown artifacts or preamble
            if stripped.startswith(('#', '*', '```')):
                continue
            cleaned_lines.append(stripped)

        champion_text = '\n'.join(cleaned_lines)
        print(f"  [Champion] Found {champion_name}: {champion_url}")
        return champion_text, champion_url

    except Exception as e:
        print(f"  [Champion] Error searching for {champion_name}: {e}")
        return None, None


def deduplicate_champion_from_contacts(contacts_text, champion_url, champion_name):
    """
    Remove any contact block from contacts_text that matches the champion
    by URL or by name (case-insensitive partial match).

    Contact blocks are separated by blank lines.

    Args:
        contacts_text: the full contacts string
        champion_url: LinkedIn URL of the champion
        champion_name: name of the champion

    Returns:
        str: contacts_text with the champion's block removed
    """
    if not contacts_text:
        return contacts_text

    # Split into blocks separated by blank lines
    blocks = re.split(r'\n\s*\n', contacts_text)
    filtered = []
    champion_lower = champion_name.lower()

    for block in blocks:
        block_stripped = block.strip()
        if not block_stripped:
            continue

        # Check URL match
        if champion_url and champion_url in block_stripped:
            continue

        # Check name match (case-insensitive, partial)
        first_line = block_stripped.split('\n')[0].strip().lower()
        if champion_lower in first_line or first_line in champion_lower:
            continue

        filtered.append(block_stripped)

    return '\n\n'.join(filtered)


def _tavily_linkedin_search(tavily_client, queries, company_name):
    """
    Run a batch of Tavily searches for LinkedIn profiles.

    Args:
        tavily_client: initialized TavilyClient
        queries: list of search query strings
        company_name: str for logging

    Returns:
        dict of {url: profile_data}
    """
    profiles = {}

    for i, query in enumerate(queries):
        try:
            print(f"    [{i+1}/{len(queries)}] Searching: {query[:60]}...")

            response = tavily_client.search(
                query=query,
                search_depth="advanced",
                include_domains=["linkedin.com"],
                max_results=5
            )

            results = response.get("results", [])
            for result in results:
                url = result.get("url", "")
                if "linkedin.com/in/" in url and url not in profiles:
                    profiles[url] = {
                        "url": url,
                        "title": result.get("title", ""),
                        "snippet": result.get("content", ""),
                        "query": query
                    }

            time.sleep(0.3)

        except Exception as e:
            print(f"    Warning: Query failed - {e}")
            continue

    return profiles


def _gemini_grounded_linkedin_search(roles, company_name):
    """
    Single Gemini grounded call to search LinkedIn for multiple roles at once.

    Args:
        roles: list of role strings (e.g. ["Director Marketing", "ABM"])
        company_name: str

    Returns:
        dict of {url: profile_data}
    """
    role_queries = "\n".join(
        f'site:linkedin.com/in "{company_name}" "{role}"' for role in roles
    )

    prompt = f"""Search LinkedIn for people currently working at {company_name} in these roles:

{role_queries}

For each person found, provide:
Name
Title: [their title]
LinkedIn: [full LinkedIn profile URL]
Snippet: [brief background from the profile]

Only include people who currently work at {company_name}. Include the actual linkedin.com/in/ URL for each person."""

    try:
        result = _call_gemini_grounded(prompt, max_tokens=4096)
        time.sleep(1)  # Gemini rate limits are tighter than Tavily
    except Exception as e:
        print(f"    Warning: Gemini grounded search failed - {e}")
        return {}

    if not result:
        return {}

    # Parse response for LinkedIn URLs and surrounding context
    profiles = {}
    lines = result.split('\n')
    for i, line in enumerate(lines):
        urls = re.findall(r'https?://(?:www\.)?linkedin\.com/in/[^\s,)]+', line)
        for url in urls:
            url = url.rstrip('.')
            if url not in profiles:
                # Grab surrounding lines as context
                context_start = max(0, i - 3)
                context_end = min(len(lines), i + 2)
                context = '\n'.join(lines[context_start:context_end])
                profiles[url] = {
                    "url": url,
                    "title": context.split('\n')[0] if context else "",
                    "snippet": context,
                    "query": f"Gemini grounded: {company_name} {roles}"
                }

    return profiles


def _merge_into_bucket(bucket, new_profiles, seen_urls):
    """
    Add new profiles to a bucket dict, skipping any URL already in seen_urls.

    Args:
        bucket: dict of {url: profile_data} to merge into
        new_profiles: dict of {url: profile_data} from a search pass
        seen_urls: set of URLs seen across all buckets (updated in place)

    Returns:
        int: number of new profiles actually added
    """
    added = 0
    for url, data in new_profiles.items():
        if url not in seen_urls:
            bucket[url] = data
            seen_urls.add(url)
            added += 1
    return added


def search_linkedin_contacts_with_tavily(company_name):
    """
    Two-bucket multi-pass search for LinkedIn contacts.

    Phase 1 - Marketing (primary, target 5-10):
      Pass 1 (Tavily): CMO, VP/SVP Marketing, ABM, Demand Gen, Growth Marketing
      Pass 2 (Tavily): Marketing Ops, MOPs, MarTech, RevOps
      Pass 3 (Gemini grounded, if < 5 marketing): Director/Product/Field/Content/Digital Marketing

    Phase 2 - Leadership (secondary, target 5):
      Pass 4 (Tavily): CEO, CRO, Founder, President
      Pass 5 (Gemini grounded, if < 5 leadership): CFO, COO, CTO, Co-Founder
      Pass 6 (Gemini grounded, if < 5 leadership): VP Sales/Revenue, Director Sales/Revenue

    Falls back to full Gemini Google Search if < 3 total valid profiles.
    Final Gemini formatting outputs marketing first (including marketing leadership), then company leadership.
    """
    config = get_config()

    # Check if Tavily is available and configured
    if not TAVILY_AVAILABLE or not config.get("tavily_api_key"):
        print("  [Tavily] Not available, falling back to Gemini Google Search...")
        return search_linkedin_contacts_with_gemini(company_name)

    print(f"  [Two-Bucket Search] Starting contact search for {company_name}...")

    try:
        tavily = TavilyClient(api_key=config["tavily_api_key"])
        marketing_profiles = {}
        leadership_profiles = {}
        seen_urls = set()

        # ── Phase 1: Marketing (primary, target 5-10) ───────────────────

        # Pass 1 (Tavily): Marketing leadership + ABM/Demand Gen
        print("\n  === Phase 1: Marketing (primary, target 5-10) ===")
        print("\n  --- Pass 1/6 (Tavily): Marketing leadership, ABM, Demand Gen ---")
        pass1_queries = [
            f'site:linkedin.com/in "{company_name}" CMO',
            f'site:linkedin.com/in "{company_name}" "Chief Marketing Officer"',
            f'site:linkedin.com/in "{company_name}" "VP Marketing"',
            f'site:linkedin.com/in "{company_name}" "SVP Marketing"',
            f'site:linkedin.com/in "{company_name}" ABM',
            f'site:linkedin.com/in "{company_name}" "Account-Based Marketing"',
            f'site:linkedin.com/in "{company_name}" "Demand Gen"',
            f'site:linkedin.com/in "{company_name}" "Demand Generation"',
            f'site:linkedin.com/in "{company_name}" "Growth Marketing"',
        ]
        pass1 = _tavily_linkedin_search(tavily, pass1_queries, company_name)
        added = _merge_into_bucket(marketing_profiles, pass1, seen_urls)
        print(f"  [Pass 1] {added} new marketing profiles ({len(marketing_profiles)} marketing total)")

        # Pass 2 (Tavily): Marketing Ops, MOPs, MarTech, RevOps
        print("\n  --- Pass 2/6 (Tavily): Marketing Ops and MarTech ---")
        pass2_queries = [
            f'site:linkedin.com/in "{company_name}" "Marketing Operations"',
            f'site:linkedin.com/in "{company_name}" "Marketing Ops"',
            f'site:linkedin.com/in "{company_name}" MOPs',
            f'site:linkedin.com/in "{company_name}" "Marketing Technology"',
            f'site:linkedin.com/in "{company_name}" "Revenue Operations"',
        ]
        pass2 = _tavily_linkedin_search(tavily, pass2_queries, company_name)
        added = _merge_into_bucket(marketing_profiles, pass2, seen_urls)
        print(f"  [Pass 2] {added} new marketing profiles ({len(marketing_profiles)} marketing total)")

        # Pass 3 (Gemini grounded, if < 5 marketing): broader marketing titles
        if len(marketing_profiles) < 5:
            print("\n  --- Pass 3/6 (Gemini grounded): Broader marketing titles ---")
            pass3_roles = [
                "Director Marketing", "Digital Marketing",
                "Product Marketing", "Field Marketing", "Content Marketing",
            ]
            pass3 = _gemini_grounded_linkedin_search(pass3_roles, company_name)
            added = _merge_into_bucket(marketing_profiles, pass3, seen_urls)
            print(f"  [Pass 3] {added} new marketing profiles ({len(marketing_profiles)} marketing total)")
        else:
            print(f"\n  --- Pass 3/6 (Gemini grounded): Skipped ({len(marketing_profiles)} marketing >= 5) ---")

        print(f"\n  [Phase 1 done] {len(marketing_profiles)} marketing contacts found")

        # ── Phase 2: Leadership (secondary, target 5) ─────────────────

        # Pass 4 (Tavily): Founders and C-suite
        print("\n  === Phase 2: Leadership (secondary, target 5) ===")
        print("\n  --- Pass 4/6 (Tavily): Founders and C-suite ---")
        pass4_queries = [
            f'site:linkedin.com/in "{company_name}" CEO',
            f'site:linkedin.com/in "{company_name}" CRO',
            f'site:linkedin.com/in "{company_name}" Founder',
            f'site:linkedin.com/in "{company_name}" President',
        ]
        pass4 = _tavily_linkedin_search(tavily, pass4_queries, company_name)
        added = _merge_into_bucket(leadership_profiles, pass4, seen_urls)
        print(f"  [Pass 4] {added} new leadership profiles ({len(leadership_profiles)} leadership total)")

        # Pass 5 (Gemini grounded, if < 5 leadership): more C-suite
        if len(leadership_profiles) < 5:
            print("\n  --- Pass 5/6 (Gemini grounded): Additional C-suite ---")
            pass5_roles = [
                "CFO", "COO", "CTO", "Co-Founder",
            ]
            pass5 = _gemini_grounded_linkedin_search(pass5_roles, company_name)
            added = _merge_into_bucket(leadership_profiles, pass5, seen_urls)
            print(f"  [Pass 5] {added} new leadership profiles ({len(leadership_profiles)} leadership total)")
        else:
            print(f"\n  --- Pass 5/6 (Gemini grounded): Skipped ({len(leadership_profiles)} leadership >= 5) ---")

        # Pass 6 (Gemini grounded, if < 5 leadership): sales/revenue leaders
        if len(leadership_profiles) < 5:
            print("\n  --- Pass 6/6 (Gemini grounded): Sales and Revenue leaders ---")
            pass6_roles = [
                "VP Sales", "VP Revenue",
                "Director Sales", "Director Revenue",
            ]
            pass6 = _gemini_grounded_linkedin_search(pass6_roles, company_name)
            added = _merge_into_bucket(leadership_profiles, pass6, seen_urls)
            print(f"  [Pass 6] {added} new leadership profiles ({len(leadership_profiles)} leadership total)")
        else:
            print(f"\n  --- Pass 6/6 (Gemini grounded): Skipped ({len(leadership_profiles)} leadership >= 5) ---")

        total = len(marketing_profiles) + len(leadership_profiles)
        print(f"\n  [Two-Bucket Search] {total} total unique profiles "
              f"({len(marketing_profiles)} marketing, {len(leadership_profiles)} leadership)")

        # Validate URLs per bucket so bucket identity survives
        marketing_profiles = validate_and_fix_linkedin_urls(marketing_profiles, company_name)
        leadership_profiles = validate_and_fix_linkedin_urls(leadership_profiles, company_name)

        total_valid = len(marketing_profiles) + len(leadership_profiles)
        print(f"  [Two-Bucket Search] {total_valid} valid profiles after URL check "
              f"({len(marketing_profiles)} marketing, {len(leadership_profiles)} leadership)")

        # Fallback: if < 3 total valid profiles, use full Gemini Google Search
        if total_valid < 3:
            print("  [Two-Bucket Search] Too few valid profiles (<3), falling back to full Gemini Google Search...")
            return search_linkedin_contacts_with_gemini(company_name)

        # Build profiles_text with bucket tags
        print("  [Gemini] Formatting contact information...")

        profiles_text = ""
        for url, data in list(marketing_profiles.items())[:15]:
            profiles_text += f"""
Bucket: MARKETING
URL: {data['url']}
Title/Name from Search: {data['title']}
Snippet: {data['snippet']}
Search Query Used: {data['query']}
---
"""
        for url, data in list(leadership_profiles.items())[:10]:
            profiles_text += f"""
Bucket: LEADERSHIP
URL: {data['url']}
Title/Name from Search: {data['title']}
Snippet: {data['snippet']}
Search Query Used: {data['query']}
---
"""

        format_prompt = f"""# ROLE
Act as an Executive Sales Researcher. Your task is to format LinkedIn contact data into a structured list with two labeled sections.

# RAW DATA
Below are LinkedIn profile URLs and snippets found for people at {company_name}. Each profile has a Bucket tag (MARKETING or LEADERSHIP):

{profiles_text}

# TASK
Using ONLY the data provided above, format each person into the contact format below.
Output TWO sections in this exact order:

Section 1: Marketing (all profiles tagged MARKETING)
  - Includes marketing leadership (CMO, VP Marketing, SVP Marketing) and practitioners
  - ABM, Demand Gen, Marketing Ops, MOPs, MarTech, RevOps, Growth Marketing, Product Marketing, etc.
  - Order by seniority within this section (CMO/VP first, then Directors, then Managers/ICs)

Then print this exact divider on its own line:
--- LEADERSHIP ---

Section 2: Leadership (all profiles tagged LEADERSHIP)
  - Founders, CEO, CRO, CFO, COO, CTO, President
  - VPs and Directors in Sales or Revenue
  - Order by seniority within this section (Founders/CEO first)

# CRITICAL FORMATTING INSTRUCTIONS
This output will be pasted into Google Docs which does NOT render markdown.
DO NOT use any markdown syntax (no **, no *, no #, no [], no ()).

Use this EXACT plain text format for each contact:

CONTACT NAME
Title: [Their Current Title at {company_name}]
LinkedIn: [Full URL exactly as provided]
Location: [City, State/Country if mentioned, otherwise "Verify on profile"]
Insight: [Brief note from the snippet about their background or expertise]

(blank line between contacts)

# RULES
1. Only include people who appear to CURRENTLY work at {company_name}
2. Use the exact LinkedIn URL provided - do not modify it
3. Extract name from the search title (usually "Name - Title | LinkedIn")
4. If information is not available in the snippet, write "Verify on profile"
5. Marketing Practitioners section MUST come before the --- LEADERSHIP --- divider
6. Aim to include all profiles provided (up to 25 total)
7. Respect the Bucket tag: do not move MARKETING profiles into the Leadership section or vice versa

Now format the contacts:"""

        gateway = LLMGateway(profile="strategic")
        result = gateway.chat(
            messages=[{"role": "user", "content": format_prompt}],
            temperature=0.3,
            max_tokens=8192,
        )

        contact_count = result.lower().count("linkedin.com/in/")
        print(f"  [Two-Bucket Search] Formatted {contact_count} contacts "
              f"({len(marketing_profiles)} marketing, {len(leadership_profiles)} leadership)")
        return result

    except Exception as e:
        print(f"  [Two-Bucket Search] Error: {e}")
        print("  [Two-Bucket Search] Falling back to Gemini Google Search...")
        return search_linkedin_contacts_with_gemini(company_name)


def validate_and_fix_linkedin_urls(profiles, company_name):
    """
    Validate LinkedIn URLs and fix encoding issues.
    LinkedIn blocks HEAD requests (405), so we check for obvious issues:
    - URL encoding problems (like %C3%BC instead of proper UTF-8)
    - 404/999 responses (invalid profiles or rate limiting)

    Args:
        profiles: dict of {url: profile_data}
        company_name: str

    Returns:
        dict of validated {url: profile_data} with corrected URLs
    """
    print(f"  [Validation] Checking {len(profiles)} LinkedIn URLs...")

    validated_profiles = {}
    fixed_count = 0
    skipped_count = 0

    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    for url, data in profiles.items():
        # Check for URL encoding issues (common problem with international names)
        has_encoding_issue = '%' in url and any(
            pattern in url for pattern in ['%C3%', '%C2%', '%E2%', '%C4%', '%C5%']
        )

        if has_encoding_issue:
            print(f"    [!] URL has encoding issues: {url[:60]}...")
            # Try to find correct URL via Google
            fixed_url = find_linkedin_url_via_google(data, company_name)
            if fixed_url and fixed_url != url:
                print(f"    [+] Found corrected URL via Google")
                data['url'] = fixed_url
                validated_profiles[fixed_url] = data
                fixed_count += 1
            else:
                # Keep original if we can't fix it
                validated_profiles[url] = data
            time.sleep(0.5)
            continue

        # For URLs without obvious issues, try a quick GET request
        # to check if the profile exists (LinkedIn blocks HEAD)
        try:
            response = requests.get(
                url,
                headers=headers,
                timeout=5,
                allow_redirects=True,
                stream=True  # Don't download full response
            )
            # Close immediately - we only need the status code
            response.close()

            # 200 = valid, 404 = not found, 999/403 = rate limited
            if response.status_code == 200:
                validated_profiles[url] = data
            elif response.status_code == 404:
                print(f"    [x] Invalid URL (HTTP 404): {url[:60]}...")
                fixed_url = find_linkedin_url_via_google(data, company_name)
                if fixed_url and fixed_url != url:
                    print(f"    [+] Found corrected URL via Google")
                    data['url'] = fixed_url
                    validated_profiles[fixed_url] = data
                    fixed_count += 1
                else:
                    skipped_count += 1
                time.sleep(0.5)
            elif response.status_code in [999, 403]:
                print(f"    [~] Rate-limited (HTTP {response.status_code}), keeping: {url[:60]}...")
                validated_profiles[url] = data
            else:
                # Other status codes - keep the URL
                validated_profiles[url] = data

        except requests.RequestException:
            # Connection errors - keep the URL (may still work for users)
            validated_profiles[url] = data

    print(f"  [Validation] {len(validated_profiles)} valid URLs ({fixed_count} corrected, {skipped_count} removed)")
    return validated_profiles


def extract_and_strip_linkedin_lines(contacts_text):
    """
    Extract LinkedIn URLs mapped to contact names and remove "LinkedIn:" lines.

    Scans the contacts text for lines starting with "LinkedIn:" that contain a URL,
    looks back 1-3 lines to find the associated contact name, builds a {name: url}
    mapping, and removes the "LinkedIn:" lines from the text.

    Returns:
        tuple: (cleaned_text, url_mappings) where url_mappings is {name: url}
    """
    if not contacts_text:
        return contacts_text, {}

    lines = contacts_text.split('\n')
    url_mappings = {}
    lines_to_remove = set()

    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped.startswith("LinkedIn:"):
            continue
        # Extract URL from the line
        url_match = re.search(r'https?://[^\s]+', stripped)
        if not url_match:
            continue
        url = url_match.group(0)

        # Look back 1-3 lines to find the contact name
        for j in range(1, 4):
            if i - j < 0:
                break
            potential_name = lines[i - j].strip()
            if (potential_name and
                not potential_name.startswith((
                    "Title:", "LinkedIn:", "Tenure:", "Location:",
                    "Insight:", "CHAMPION",
                ))):
                url_mappings[potential_name] = url
                break

        lines_to_remove.add(i)

    cleaned_lines = [line for idx, line in enumerate(lines) if idx not in lines_to_remove]
    cleaned_text = '\n'.join(cleaned_lines)

    return cleaned_text, url_mappings


def find_linkedin_url_via_google(profile_data, company_name):
    """
    Attempt to find correct LinkedIn URL by searching Google.
    Extracts name and title from profile data, searches Google, parses results.

    Args:
        profile_data: dict with 'title', 'snippet', 'url'
        company_name: str

    Returns:
        str: corrected LinkedIn URL or None
    """
    try:
        # Extract name from title (usually "Name - Title | LinkedIn" or similar)
        title = profile_data.get('title', '')

        # Common patterns in LinkedIn search results:
        # "Peter Juntgen - VP Marketing | LinkedIn"
        # "Jane Smith | LinkedIn"
        name = ''
        if ' - ' in title:
            name = title.split(' - ')[0].strip()
        elif '|' in title:
            name = title.split('|')[0].strip()
        else:
            # Fallback: use the whole title minus common suffixes
            name = title.replace('| LinkedIn', '').replace('- LinkedIn', '').strip()

        if not name or len(name) < 3:
            return None

        # Build a Google search query
        search_query = f'"{name}" "{company_name}" site:linkedin.com/in'

        # Use Google search via requests
        search_url = f"https://www.google.com/search?q={quote_plus(search_query)}&num=3"
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }

        response = requests.get(search_url, headers=headers, timeout=10)
        response.raise_for_status()

        # Parse HTML to find LinkedIn URLs
        soup = BeautifulSoup(response.text, 'html.parser')

        # Look for LinkedIn URLs in search results
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Google wraps URLs in /url?q=...
            if '/url?q=' in href:
                actual_url = href.split('/url?q=')[1].split('&')[0]
                if 'linkedin.com/in/' in actual_url:
                    return actual_url
            elif 'linkedin.com/in/' in href:
                return href

        return None

    except Exception as e:
        # Silently fail - we'll just skip this profile
        return None


# =============================================================================
# TAVILY NEWS & RECENT ACTIVITY
# =============================================================================

def generate_news_and_activity(company_name, domain):
    """
    Use Tavily to find recent news and activity about the company.
    Returns plain text formatted for Google Docs (no markdown).
    """
    config = get_config()

    # Check if Tavily is available and configured
    if not TAVILY_AVAILABLE or not config.get("tavily_api_key"):
        print("  [News] Tavily not available - skipping news section")
        return "News section requires Tavily API. Please configure TAVILY_API_KEY."

    print(f"  [Tavily] Searching for recent news about {company_name}...")

    try:
        tavily = TavilyClient(api_key=config["tavily_api_key"])

        # Define search queries for different types of news
        news_queries = [
            f'"{company_name}" news announcement 2026 2025',
            f'"{company_name}" press release',
            f'"{company_name}" funding round investment',
            f'"{company_name}" product launch announcement',
            f'"{company_name}" partnership announcement',
            f'"{company_name}" executive hire CEO CMO CRO',
        ]

        all_news = {}  # URL -> news item (deduplication)

        for query in news_queries:
            try:
                response = tavily.search(
                    query=query,
                    search_depth="advanced",
                    max_results=5
                )

                results = response.get("results", [])
                for result in results:
                    url = result.get("url", "")
                    if url not in all_news:
                        # Extract domain for source name
                        source = url.split("/")[2] if "/" in url else "Unknown"
                        source = source.replace("www.", "").split(".")[0].title()

                        all_news[url] = {
                            "url": url,
                            "title": result.get("title", ""),
                            "content": result.get("content", ""),
                            "source": source,
                            "published_date": result.get("published_date", "")
                        }

                time.sleep(0.2)

            except Exception as e:
                print(f"    Warning: News query failed - {e}")
                continue

        print(f"  [Tavily] Found {len(all_news)} news items")

        if not all_news:
            return "No recent news found. Consider manual research for this company."

        # Format news items using Gemini for consistent output
        news_text = ""
        for url, item in list(all_news.items())[:15]:  # Limit to 15 items
            news_text += f"""
Title: {item['title']}
Source: {item['source']}
URL: {item['url']}
Content: {item['content'][:500]}
Published: {item['published_date'] or 'Unknown'}
---
"""

        format_prompt = f"""# ROLE
You are a business research analyst summarizing recent news about {company_name}.

# RAW NEWS DATA
{news_text}

# TASK
Format the most relevant news items (5-10 items max) into a clean summary.

# CRITICAL FORMATTING INSTRUCTIONS
This output will be pasted into Google Docs which does NOT render markdown.
DO NOT use any markdown syntax (no **, no *, no #, no [], no ()).

Use this EXACT plain text format for each news item:

[Date or "Recent"] - [Headline]
Source: [Publication Name]
Summary: [2-3 sentence summary of the key points]

(blank line between items)

# RULES
1. Order by relevance and recency (most important/recent first)
2. Include 5-10 items maximum
3. Skip duplicate stories or very similar items
4. Focus on business-relevant news: funding, partnerships, products, executive changes
5. Write clear, factual summaries without hype
6. If date is unknown, use "Recent" as the date

Now format the news items:"""

        gateway = LLMGateway(profile="strategic")
        result = gateway.chat(
            messages=[{"role": "user", "content": format_prompt}],
            temperature=0.3,
            max_tokens=4096,
        )

        news_count = result.count("Source:")
        print(f"  [News] Formatted {news_count} news items")
        return result

    except Exception as e:
        print(f"  [News] Error: {e}")
        return f"Error fetching news: {str(e)}"


# =============================================================================
# GEMINI API INTEGRATION
# =============================================================================

def call_gemini_api(prompt, max_tokens=16384, use_search=False):
    """Call Gemini API for text generation with optional Google Search grounding.

    Standard calls route through LLMGateway (OpenAI-compatible).
    Grounded search calls use the native Gemini REST API since
    google_search is a vendor-specific tool not in the OpenAI spec.
    """
    if use_search:
        return _call_gemini_grounded(prompt, max_tokens)

    try:
        gateway = LLMGateway(profile="strategic")
        return gateway.chat(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.4,
            max_tokens=max_tokens,
        )
    except Exception as e:
        print(f"  [Gemini/Gateway] Error: {e}")
        return ""


def _call_gemini_grounded(prompt, max_tokens=16384):
    """Native Gemini REST call for google_search grounding (not OpenAI-compatible).

    Retries up to 4 attempts with increasing back-off on rate-limit, server errors,
    and silent empty-response failures.
    """
    config = get_config()
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={config['gemini_api_key']}"

    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": 0.4,
        },
        "tools": [{"google_search": {}}],
    }

    waits = [0, 15, 30, 60]  # seconds to wait before each attempt (first is immediate)
    for attempt, wait in enumerate(waits, start=1):
        if wait > 0:
            print(f"  [Gemini/Grounded] Retrying in {wait}s (attempt {attempt}/{len(waits)})...")
            time.sleep(wait)

        try:
            response = requests.post(
                url, headers={"Content-Type": "application/json"}, json=payload, timeout=180
            )
            response.raise_for_status()
            data = response.json()

            candidates = data.get("candidates", [])
            if not candidates:
                print("  [Gemini/Grounded] Warning: empty candidates in response")
                if attempt < len(waits):
                    continue
                return ""

            parts = candidates[0].get("content", {}).get("parts", [])
            text_parts = [p["text"] for p in parts if "text" in p]
            if not text_parts:
                finish = candidates[0].get("finishReason", "?")
                print(f"  [Gemini/Grounded] Warning: no text parts (finishReason={finish})")
                if attempt < len(waits):
                    continue
                return ""

            return "\n".join(text_parts)

        except requests.exceptions.HTTPError as e:
            status = e.response.status_code if e.response else "?"
            body = e.response.text[:200] if e.response else ""
            print(f"  [Gemini/Grounded] HTTP {status} error: {e} — {body}")
            retriable = status == 429 or (isinstance(status, int) and 500 <= status < 600)
            if not retriable or attempt >= len(waits):
                return ""
        except requests.exceptions.Timeout:
            print("  [Gemini/Grounded] Timeout after 180s")
            if attempt >= len(waits):
                return ""
        except Exception as e:
            print(f"  [Gemini/Grounded] Unexpected error: {type(e).__name__}: {e}")
            return ""

    return ""


def _brave_search(query, count=5):
    """Brave web search. Returns list of {title, url, description} dicts."""
    config = get_config()
    brave_api_key = config.get("brave_api_key")
    if not brave_api_key:
        return []
    headers = {"X-Subscription-Token": brave_api_key, "Accept": "application/json"}
    try:
        resp = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers=headers,
            params={"q": query, "count": count},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("web", {}).get("results", [])
    except Exception as e:
        print(f"  [Brave] Search error for '{query[:60]}': {e}")
        return []


def _gather_company_data_brave(company_name, domain):
    """Run targeted Brave Search queries and return a raw context block for LLM synthesis."""
    print(f"  [Brave] Gathering context for {company_name}...")
    queries = [
        f'"{company_name}" company overview crunchbase',
        f'"{company_name}" funding history investors',
        f'"{company_name}" revenue employees valuation site:crunchbase.com OR site:pitchbook.com',
        f'"{company_name}" news 2025 2026 announcement',
    ]
    all_snippets = []
    seen_urls: set = set()
    for query in queries:
        results = _brave_search(query, count=5)
        for r in results:
            url = r.get("url", "")
            if url in seen_urls:
                continue
            seen_urls.add(url)
            snippet = (
                f"Title: {r.get('title', '')}\n"
                f"URL: {url}\n"
                f"Description: {r.get('description', '')}"
            )
            all_snippets.append(snippet)

    if not all_snippets:
        return ""

    print(f"  [Brave] Gathered {len(all_snippets)} snippets")
    return "\n\n---\n\n".join(all_snippets)


def generate_company_research(apollo_data, company_name):
    """Generate Company Research section using Gemini with exact template prompt."""
    print("  [Gemini] Generating Company Research section...")

    prompt = f"""# ROLE
Act as a Senior Investment Analyst specializing in Venture Capital and Private Equity research. Your goal is to provide a comprehensive, data-driven "Deep Dive" report on {company_name}.

# TASK
Research and synthesize the following information into a structured report. If specific data points are private or unavailable, provide your best professional estimate based on market proxies, headcount, and industry benchmarks.

### 1. Snapshot & Market Presence
- LinkedIn Employee Count: Current headcount and 1-year growth trend if visible.
- Estimated Annual Revenue: Provide a range. Explain your reasoning (e.g., "Based on a $500k ACV and 200 known customers...").
- Estimated Company Value: Current valuation (Post-money if private) or Market Cap (if public).
- Status: (e.g., Late-stage Private, Public, Subsidiary).

### 2. Funding & Capital Structure
- Total Funding Raised: Cumulative amount to date.
- Latest Funding Round: Date, Series, and Amount.
- History of Funding/Value: For Private: List major rounds (Seed through current) with dates and lead investors. For Public: Enterprise Value (EV) changes over the last 3 years and IPO Year.
- Key Investors: List the top 5-7 institutional investors (VCs, PEs, or Sovereign Wealth Funds).

### 3. Business Model & Operations
- Revenue Model: Detail exactly how they make money (SaaS, RaaS, Transactional, Hardware sales, etc.).
- Key Customer Logos: List major, recognizable customers or partners.
- Operational Scale: Sites, units deployed, or "milestone" metrics (e.g., "6 billion picks").

### 4. Company Narrative & Product
- History: A brief timeline of the company's origin, including founders and pivot points.
- Product Overview: What do they build? Describe the core technology/service.
- Product Differentiation: What makes them different from competitors? (e.g., "Physical AI," "Zero-touch fulfillment").

# CRITICAL FORMATTING INSTRUCTIONS
This output will be pasted into Google Docs which does NOT render markdown. Follow these rules:

1. DO NOT use any markdown syntax:
   - No # or ## for headers
   - No ** for bold
   - No | pipes for tables
   - No ``` code blocks
   - No bullet point symbols like - or *

2. USE THIS EXACT PLAIN TEXT FORMAT:

EXECUTIVE SUMMARY

[Write 2-3 paragraphs of narrative prose summarizing the company]

1. SNAPSHOT AND MARKET PRESENCE

LinkedIn Employee Count: [number] employees ([X]% growth YoY)
[One sentence explaining the data source or trend]

Estimated Annual Revenue: $[X]M - $[Y]M
[One sentence explaining your reasoning]

Estimated Company Value: $[X]B (Post-money valuation as of [date])
[One sentence on valuation basis]

Status: [Late-stage Private / Public / etc.]

2. FUNDING AND CAPITAL STRUCTURE

Total Funding Raised: $[X]M

Latest Funding Round: Series [X], $[amount], [date]

Funding History:
[Date] - Seed - $[X]M - Led by [Investor]
[Date] - Series A - $[X]M - Led by [Investor]
[Date] - Series B - $[X]M - Led by [Investor]
(continue for all rounds)

Key Investors: [Investor 1], [Investor 2], [Investor 3], [Investor 4], [Investor 5]

3. BUSINESS MODEL AND OPERATIONS

Revenue Model: [Describe how they make money in 1-2 sentences]

Key Customer Logos: [Customer 1], [Customer 2], [Customer 3], [Customer 4], [Customer 5]

Operational Scale: [Key metrics like units deployed, sites, volume processed]

4. COMPANY NARRATIVE AND PRODUCT

History: [2-3 sentences on founding, founders, and key milestones]

Product Overview: [2-3 sentences describing what they build/sell]

Product Differentiation: [2-3 sentences on competitive advantages]

# INSTRUCTION ON UNCERTAINTY
If the company is private and revenue is not disclosed, look for "proxies" such as:
1. Average Contract Value (ACV) for the industry.
2. Revenue per employee benchmarks for similar sectors.
3. Recent press releases regarding "growth percentages."

Now generate the complete report for {company_name}:"""

    # Primary: Gemini with Google Search grounding
    result = call_gemini_api(prompt, use_search=True)
    if result:
        return result

    # Fallback: Brave Search + LLMGateway (non-grounded, different provider/path)
    print("  [Company Research] Gemini grounded returned empty — trying Brave Search fallback...")
    domain = apollo_data.get("domain", "") if apollo_data else ""
    context = _gather_company_data_brave(company_name, domain)
    if not context:
        print("  [Company Research] Brave fallback has no context — giving up")
        return ""

    fallback_prompt = (
        "# RAW CONTEXT FROM WEB SEARCH\n\n"
        + context
        + "\n\n# TASK\n"
        + f"Using the context above (and your own knowledge), generate the Company Research report for {company_name}.\n\n"
        + prompt
    )

    try:
        gateway = LLMGateway(profile="strategic")
        result = gateway.chat(
            messages=[{"role": "user", "content": fallback_prompt}],
            temperature=0.4,
            max_tokens=16384,
        )
        if result:
            print("  [Company Research] Brave fallback succeeded")
        return result
    except Exception as e:
        print(f"  [Company Research] Brave fallback LLM call failed: {e}")
        return ""


def generate_techstack_analysis(apollo_tech, scraped_tech, company_name):
    """Generate TechStack section using Gemini."""
    print("  [Gemini] Generating TechStack section...")

    # Combine tech data
    all_tech = []

    # Add Apollo tech stack
    for tech in apollo_tech:
        all_tech.append(f"{tech['name']} (Category: {tech['category']}) [Source: Apollo]")

    # Add scraped tech
    for tech in scraped_tech:
        all_tech.append(f"{tech} [Source: Website Scrape]")

    tech_data = "\n".join(all_tech) if all_tech else "No technologies detected"

    prompt = f"""# ROLE
Act as a Marketing Operations (MOPs) and Revenue Operations (RevOps) technologist. Your goal is to analyze raw data signatures and extract a confirmed "MarTech & RevTech Stack."

# TASK
1. Analyze the raw text/JSON provided below.
2. Identify software and tools that specifically fit into Marketing or Sales/Revenue functions.
3. IGNORE all non-revenue technology (e.g., HR, Engineering, Security, IT Infrastructure, Cloud Hosting).
4. Categorize the findings into the specific "Functional Buckets" listed below.

# RAW DATA INPUT FOR {company_name}
{tech_data}

# CRITICAL FORMATTING INSTRUCTIONS
This output will be pasted into Google Docs which does NOT render markdown.
DO NOT use any markdown syntax (no **, no *, no #, no pipes |).

Use this EXACT plain text format:

CRM
[Tool Name 1], [Tool Name 2]

Marketing Automation (MAP)
[Tool Name 1], [Tool Name 2]

ABM & Intent
[Tool Name 1], [Tool Name 2]

Sales Engagement (SEP)
[Tool Name 1], [Tool Name 2]

Conversational / Chat
[Tool Name 1], [Tool Name 2]

CMS / Web
[Tool Name 1], [Tool Name 2]

Analytics & Attribution
[Tool Name 1], [Tool Name 2]

Event / Webinar
[Tool Name 1], [Tool Name 2]

Other RevTech
[Tool Name 1], [Tool Name 2]

RULES:
- Only include categories where tools were found
- List tool names separated by commas
- If a category has no tools, omit that entire section
- If the same tool appears multiple times, list it only once

Generate the categorized tech stack analysis now:"""

    return call_gemini_api(prompt)


def generate_contacts_analysis(contacts_data, company_name):
    """Generate Contacts section using Gemini."""
    print("  [Gemini] Generating Contacts section...")

    if not contacts_data:
        return "No LinkedIn contacts found via search. Manual research recommended."

    # Format contacts data for the prompt
    contacts_text = ""
    for contact in contacts_data[:20]:  # Limit to top 20
        contacts_text += f"""
URL: {contact.get('url', '')}
Title/Name: {contact.get('title', '')}
Snippet: {contact.get('snippet', '')}
Search Query: {contact.get('query', '')}
---
"""

    prompt = f"""# ROLE
Act as an Executive Sales Researcher. Your goal is to identify high-value decision-makers at **{company_name}** using the search results provided.

# TASK
Analyze the search results below and format them into a structured contact list. Focus on:

1. **Corporate Leadership:** C-Suite (CEO, CMO, CRO, CFO, COO) and Founders.
2. **Marketing Leadership:** All VPs and Directors within the Marketing function.
3. **Specialists:** Any individual (regardless of seniority) with "ABM", "ABX", "Demand Generation", or "Digital Marketing" in their title.

# FILTERING CRITERIA
* **Target:** Marketing Directors+, VPs, C-Suite, and ABM/Demand Gen practitioners.
* **Exclude:** Interns, Assistants, or non-marketing Directors (e.g., "Director of Engineering") unless they are C-Suite.

# DATA EXTRACTION & FORMATTING
For each identified person, provide the following entry. If exact dates for tenure are not visible in the search snippet, estimate based on the "Experience" preview or mark as "Verify".

**[Name](LinkedIn URL)**
**Title:** [Current Title]
**Tenure (Role):** [Time in current specific role]
**Tenure (Company):** [Total time at the company]
**Location:** [City, State/Country]
*Insight: [Italicize any snippet info regarding their MarTech experience (Salesforce, Marketo, etc.), specific campaigns, or key responsibilities.]*

---

# SEARCH RESULTS
{contacts_text}

# INSTRUCTIONS ON TENURE
Since you cannot browse live profiles, look for search snippets that say "Jan 2020 - Present · 3 yrs 4 mos" to calculate tenure.
* **Role Tenure:** How long they have held their *current* title.
* **Company Tenure:** How long they have been at the company *total*.
* *If the distinction is not clear from the search result, provide the Total Tenure and note "(Total)".*

Generate the formatted contacts list now:"""

    return call_gemini_api(prompt)


# =============================================================================
# GOOGLE DOCS API INTEGRATION
# =============================================================================

def apply_text_formatting(docs_service, doc_id, full_text, url_mappings=None):
    """Apply bold, italic, and hyperlink formatting to the document text."""
    format_requests = []

    # Define patterns to make bold (labels followed by colons)
    bold_labels = [
        # Company Research section
        "LinkedIn Employee Count:",
        "Estimated Annual Revenue:",
        "Estimated Company Value:",
        "Status:",
        "Total Funding Raised:",
        "Latest Funding Round:",
        "Funding History:",
        "Key Investors:",
        "Revenue Model:",
        "Key Customer Logos:",
        "Operational Scale:",
        "History:",
        "Product Overview:",
        "Product Differentiation:",
        # Contacts section
        "CHAMPION",
        "Title:",
        "Tenure:",
        "Location:",
        "Insight:",
        # News & Recent Activity section
        "Source:",
        "Summary:",
    ]

    # Apply bold to labels
    for label in bold_labels:
        start = 0
        while True:
            pos = full_text.find(label, start)
            if pos == -1:
                break
            format_requests.append({
                "updateTextStyle": {
                    "range": {
                        "startIndex": pos + 1,  # +1 for document index offset
                        "endIndex": pos + 1 + len(label)
                    },
                    "textStyle": {"bold": True},
                    "fields": "bold"
                }
            })
            start = pos + len(label)

    # Find and format subsection headers as HEADING_2
    subsection_headers = [
        "EXECUTIVE SUMMARY",
        "1. SNAPSHOT AND MARKET PRESENCE",
        "2. FUNDING AND CAPITAL STRUCTURE",
        "3. BUSINESS MODEL AND OPERATIONS",
        "4. COMPANY NARRATIVE AND PRODUCT",
    ]

    for header in subsection_headers:
        pos = full_text.find(header)
        if pos != -1:
            # Find end of line
            end_pos = full_text.find("\n", pos)
            if end_pos == -1:
                end_pos = pos + len(header)
            format_requests.append({
                "updateParagraphStyle": {
                    "range": {
                        "startIndex": pos + 1,
                        "endIndex": end_pos + 2
                    },
                    "paragraphStyle": {"namedStyleType": "HEADING_2"},
                    "fields": "namedStyleType"
                }
            })

    # Apply hyperlinks to contact names using pre-extracted url_mappings
    # Scope search to the Contacts section to avoid matching names in Company Research
    contacts_section_start = full_text.find("Contacts\n")
    if contacts_section_start == -1:
        contacts_section_start = 0

    if url_mappings:
        for name, url in url_mappings.items():
            pos = full_text.find(name, contacts_section_start)
            if pos == -1:
                continue
            format_requests.append({
                "updateTextStyle": {
                    "range": {
                        "startIndex": pos + 1,
                        "endIndex": pos + 1 + len(name)
                    },
                    "textStyle": {
                        "link": {"url": url},
                        "foregroundColor": {
                            "color": {
                                "rgbColor": {"blue": 0.8, "green": 0.2, "red": 0.1}
                            }
                        }
                    },
                    "fields": "link,foregroundColor"
                }
            })

    return format_requests


def create_google_doc(company_name, company_research, techstack, contacts, news_and_activity="", url_mappings=None):
    """Create a Google Doc with the research content."""
    print("\n[Step 6/6] Creating Google Doc...")

    # Build services
    docs_service = build_service("docs", "v1")
    drive_service = build_service("drive", "v3")

    # Create document title
    current_date = datetime.now().strftime("%B %Y")
    doc_title = f"{company_name} - Deal Notes - {current_date}"

    # Create the document
    print(f"  [Google] Creating document: {doc_title}")
    doc = docs_service.documents().create(body={"title": doc_title}).execute()
    doc_id = doc.get("documentId")

    # Move to specified folder
    config = get_config()
    try:
        file = drive_service.files().get(fileId=doc_id, fields="parents").execute()
        previous_parents = ",".join(file.get("parents", []))
        drive_service.files().update(
            fileId=doc_id,
            addParents=config["google_drive_folder_id"],
            removeParents=previous_parents,
            fields="id, parents"
        ).execute()
        print(f"  [Google] Moved to Deal Notes folder")
    except Exception as e:
        print(f"  [Google] Warning: Could not move to folder: {e}")

    # Define sections with their content and styles
    # TITLE for doc title, HEADING_1 for main sections, content has no style (formatted separately)
    sections = [
        {"text": f"{company_name} - Deal Notes - {current_date}\n", "style": "TITLE"},
        {"text": "\n", "style": None},
        {"text": "Links for Additional Docs\n", "style": "HEADING_1"},
        {"text": "I will add all of these links manually as I go\n\n", "style": None},
        {"text": "Folloze Pricing\n", "style": "HEADING_1"},
        {"text": "I will manually add the pricing notes\n\n", "style": None},
        {"text": "Company Research\n", "style": "HEADING_1"},
        {"text": company_research + "\n\n", "style": None},
        {"text": "TechStack\n", "style": "HEADING_1"},
        {"text": techstack + "\n\n", "style": None},
        {"text": "Contacts\n", "style": "HEADING_1"},
        {"text": contacts + "\n\n", "style": None},
        {"text": "News & Recent Activity\n", "style": "HEADING_1"},
        {"text": news_and_activity + "\n\n", "style": None},
        {"text": "Call Notes\n", "style": "HEADING_1"},
        {"text": "Notes from Granola will be added here via Zapier integration.\n", "style": None},
    ]

    # First pass: insert all text
    full_text = "".join([s["text"] for s in sections])

    docs_service.documents().batchUpdate(
        documentId=doc_id,
        body={"requests": [{
            "insertText": {
                "location": {"index": 1},
                "text": full_text,
            }
        }]}
    ).execute()

    # Second pass: apply heading styles to sections
    format_requests = []
    current_index = 1

    for section in sections:
        text_len = len(section["text"])
        if section["style"]:
            newline_pos = section["text"].find("\n")
            if newline_pos == -1:
                newline_pos = text_len

            format_requests.append({
                "updateParagraphStyle": {
                    "range": {
                        "startIndex": current_index,
                        "endIndex": current_index + newline_pos + 1
                    },
                    "paragraphStyle": {"namedStyleType": section["style"]},
                    "fields": "namedStyleType",
                }
            })
        current_index += text_len

    # Apply section heading styles
    if format_requests:
        try:
            docs_service.documents().batchUpdate(
                documentId=doc_id,
                body={"requests": format_requests}
            ).execute()
        except Exception as e:
            print(f"  [Google] Warning: Could not apply heading styles: {e}")

    # Third pass: apply text formatting (bold labels, hyperlinks, subsection headers)
    try:
        text_format_requests = apply_text_formatting(docs_service, doc_id, full_text, url_mappings=url_mappings)
        if text_format_requests:
            # Process in batches to avoid API limits
            batch_size = 50
            for i in range(0, len(text_format_requests), batch_size):
                batch = text_format_requests[i:i + batch_size]
                docs_service.documents().batchUpdate(
                    documentId=doc_id,
                    body={"requests": batch}
                ).execute()
    except Exception as e:
        print(f"  [Google] Warning: Could not apply text formatting: {e}")

    doc_url = f"https://docs.google.com/document/d/{doc_id}/edit"
    print(f"  [Google] Document created successfully!")

    return doc_url


# =============================================================================
# RELIABILITY REFIT
# =============================================================================

SEARCH_PROVIDER_ORDER = ("tavily", "brave", "perplexity")
SYNTHESIS_PROFILE_ORDER = ("workhorse", "local", "openai")
REQUIRED_SECTION_NAMES = ("company_research", "techstack", "contacts")
OPTIONAL_SECTION_NAMES = ("news",)
JSON_RESULT_PREFIX = "RUN_RESULT_JSON:"


@dataclass
class EvidenceItem:
    title: str
    url: str
    snippet: str
    query: str
    provider_name: str
    surface: str
    published_date: str = ""


@dataclass
class ProviderFailure:
    provider_type: str
    provider_name: str
    error_class: str
    error_message: str
    http_status: int | None = None
    section: str = ""
    fallback_hop: int = 0


@dataclass
class SectionResult:
    name: str
    status: str
    content: str = ""
    raw_evidence: list[dict[str, Any]] = field(default_factory=list)
    provider_chain: list[str] = field(default_factory=list)
    provider_failures: list[dict[str, Any]] = field(default_factory=list)
    url_mappings: dict[str, str] = field(default_factory=dict)
    notes: str = ""


@dataclass
class ResearchRunResult:
    run_id: str
    company_name: str
    domain: str
    champion_name: str | None
    status: str
    doc_created: bool
    doc_url: str
    blocked_reason: str
    failed_sections: list[str]
    degraded_sections: list[str]
    provider_failures: list[dict[str, Any]]
    sections: dict[str, SectionResult]
    jsonl_log_path: str
    created_at: str


class JsonlLogger:
    def __init__(self, run_id: str, company_name: str, domain: str) -> None:
        configured_path = os.environ.get("DEAL_RESEARCH_JSONL_PATH")
        if configured_path:
            self.path = Path(configured_path).expanduser()
        else:
            self.path = (
                Path("~/.local/share/deal-research/logs").expanduser()
                / f"{run_id}.jsonl"
            )
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.base_fields = {
            "run_id": run_id,
            "company_name": company_name,
            "domain": domain,
        }

    def log(self, **fields: Any) -> None:
        payload = {
            "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            **self.base_fields,
            **fields,
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, sort_keys=True) + "\n")


class ProviderExecutionError(RuntimeError):
    def __init__(
        self,
        provider_type: str,
        provider_name: str,
        error_class: str,
        error_message: str,
        *,
        http_status: int | None = None,
    ) -> None:
        super().__init__(error_message)
        self.provider_type = provider_type
        self.provider_name = provider_name
        self.error_class = error_class
        self.error_message = error_message
        self.http_status = http_status


def _classify_error(message: str, http_status: int | None = None) -> str:
    normalized = message.lower()
    if http_status in (401, 403) or "unauthorized" in normalized or "forbidden" in normalized:
        return "auth"
    if http_status == 429 or any(token in normalized for token in ("quota", "rate limit", "plan limit", "credits")):
        return "quota"
    if http_status and 500 <= http_status < 600:
        return "server_error"
    if "timeout" in normalized or "timed out" in normalized:
        return "timeout"
    if any(token in normalized for token in ("missing", "not set", "not installed", "not found", "config")):
        return "config"
    return "unknown"


def _should_disable_for_run(error_class: str) -> bool:
    return error_class in {"auth", "quota", "config"}


def _normalize_force_failures(values: list[str]) -> set[str]:
    forced: set[str] = set()
    for raw in values:
        for token in raw.split(","):
            token = token.strip().lower()
            if token:
                forced.add(token)
    return forced


def _is_forced_failure(force_failures: set[str], provider_type: str, provider_name: str) -> bool:
    return (
        provider_name.lower() in force_failures
        or f"{provider_type}:{provider_name}".lower() in force_failures
    )


def _dedupe_evidence(items: list[EvidenceItem]) -> list[EvidenceItem]:
    deduped: list[EvidenceItem] = []
    seen: set[str] = set()
    for item in items:
        dedupe_key = item.url or f"{item.title}|{item.query}"
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        deduped.append(item)
    return deduped


def _evidence_to_block(items: list[EvidenceItem], *, max_items: int = 12) -> str:
    blocks = []
    for item in items[:max_items]:
        blocks.append(
            "\n".join(
                [
                    f"Provider: {item.provider_name}",
                    f"Query: {item.query}",
                    f"Title: {item.title}",
                    f"URL: {item.url}",
                    f"Published: {item.published_date or 'Unknown'}",
                    f"Snippet: {item.snippet or 'No snippet'}",
                ]
            )
        )
    return "\n\n---\n\n".join(blocks)


def _provider_failure(
    exc: ProviderExecutionError,
    *,
    section: str,
    fallback_hop: int,
) -> ProviderFailure:
    return ProviderFailure(
        provider_type=exc.provider_type,
        provider_name=exc.provider_name,
        error_class=exc.error_class,
        error_message=exc.error_message,
        http_status=exc.http_status,
        section=section,
        fallback_hop=fallback_hop,
    )


def _run_tavily_search(
    queries: list[str],
    *,
    surface: str,
    domains: list[str] | None = None,
    max_results: int = 5,
) -> list[EvidenceItem]:
    config = get_config()
    if not TAVILY_AVAILABLE:
        raise ProviderExecutionError("search", "tavily", "config", "tavily package not installed")
    if not config.get("tavily_api_key"):
        raise ProviderExecutionError("search", "tavily", "config", "TAVILY_API_KEY not set")

    client = TavilyClient(api_key=config["tavily_api_key"])
    evidence: list[EvidenceItem] = []
    for query in queries:
        try:
            payload: dict[str, Any] = {
                "query": query,
                "search_depth": "advanced",
                "max_results": max_results,
            }
            if domains:
                payload["include_domains"] = domains
            response = client.search(**payload)
        except Exception as exc:
            raise ProviderExecutionError(
                "search",
                "tavily",
                _classify_error(str(exc)),
                str(exc),
            ) from exc

        for result in response.get("results", []):
            evidence.append(
                EvidenceItem(
                    title=result.get("title", ""),
                    url=result.get("url", ""),
                    snippet=result.get("content", ""),
                    published_date=result.get("published_date", ""),
                    query=query,
                    provider_name="tavily",
                    surface=surface,
                )
            )
        time.sleep(0.2)

    return _dedupe_evidence(evidence)


def _run_brave_search_batch(
    queries: list[str],
    *,
    surface: str,
    domains: list[str] | None = None,
    max_results: int = 5,
) -> list[EvidenceItem]:
    config = get_config()
    if not config.get("brave_api_key"):
        raise ProviderExecutionError("search", "brave", "config", "BRAVE_API_KEY not set")

    headers = {
        "X-Subscription-Token": config["brave_api_key"],
        "Accept": "application/json",
    }
    evidence: list[EvidenceItem] = []
    for query in queries:
        query_text = query
        if domains and len(domains) == 1 and f"site:{domains[0]}" not in query:
            query_text = f"site:{domains[0]} {query}"
        try:
            response = requests.get(
                "https://api.search.brave.com/res/v1/web/search",
                headers=headers,
                params={"q": query_text, "count": max_results},
                timeout=20,
            )
            response.raise_for_status()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response else None
            raise ProviderExecutionError(
                "search",
                "brave",
                _classify_error(str(exc), status),
                str(exc),
                http_status=status,
            ) from exc
        except requests.RequestException as exc:
            raise ProviderExecutionError(
                "search",
                "brave",
                _classify_error(str(exc)),
                str(exc),
            ) from exc

        for result in response.json().get("web", {}).get("results", []):
            evidence.append(
                EvidenceItem(
                    title=result.get("title", ""),
                    url=result.get("url", ""),
                    snippet=result.get("description", ""),
                    query=query_text,
                    provider_name="brave",
                    surface=surface,
                )
            )

    return _dedupe_evidence(evidence)


def _run_perplexity_search(
    queries: list[str],
    *,
    surface: str,
    domains: list[str] | None = None,
    max_results: int = 5,
) -> list[EvidenceItem]:
    config = get_config()
    api_key = config.get("perplexity_api_key")
    if not api_key:
        raise ProviderExecutionError("search", "perplexity", "config", "PERPLEXITY_API_KEY not set")

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    evidence: list[EvidenceItem] = []
    for query in queries:
        payload: dict[str, Any] = {
            "query": query,
            "max_results": max_results,
            "max_tokens_per_page": 1024,
        }
        if domains:
            payload["search_domain_filter"] = domains
        try:
            response = requests.post(
                "https://api.perplexity.ai/search",
                headers=headers,
                json=payload,
                timeout=30,
            )
            response.raise_for_status()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response else None
            raise ProviderExecutionError(
                "search",
                "perplexity",
                _classify_error(str(exc), status),
                str(exc),
                http_status=status,
            ) from exc
        except requests.RequestException as exc:
            raise ProviderExecutionError(
                "search",
                "perplexity",
                _classify_error(str(exc)),
                str(exc),
            ) from exc

        for result in response.json().get("results", []):
            evidence.append(
                EvidenceItem(
                    title=result.get("title", ""),
                    url=result.get("url", ""),
                    snippet=result.get("snippet", ""),
                    published_date=result.get("date", ""),
                    query=query,
                    provider_name="perplexity",
                    surface=surface,
                )
            )

    return _dedupe_evidence(evidence)


SEARCH_PROVIDER_EXECUTORS = {
    "tavily": _run_tavily_search,
    "brave": _run_brave_search_batch,
    "perplexity": _run_perplexity_search,
}


def _search_with_fallbacks(
    section_name: str,
    queries: list[str],
    *,
    logger: JsonlLogger,
    provider_health: dict[str, str],
    force_failures: set[str],
    domains: list[str] | None = None,
    max_results: int = 5,
) -> tuple[list[EvidenceItem], list[str], list[ProviderFailure]]:
    evidence: list[EvidenceItem] = []
    attempted_providers: list[str] = []
    provider_failures: list[ProviderFailure] = []

    for fallback_hop, provider_name in enumerate(SEARCH_PROVIDER_ORDER):
        health_key = f"search:{provider_name}"
        if provider_health.get(health_key) == "disabled_for_run":
            logger.log(
                section=section_name,
                provider_type="search",
                provider_name=provider_name,
                fallback_hop=fallback_hop,
                status="skipped_disabled",
            )
            continue

        attempted_providers.append(provider_name)
        start_time = time.time()
        try:
            if _is_forced_failure(force_failures, "search", provider_name):
                raise ProviderExecutionError(
                    "search",
                    provider_name,
                    "forced_failure",
                    f"forced failure for {provider_name}",
                )
            evidence = SEARCH_PROVIDER_EXECUTORS[provider_name](
                queries,
                surface=section_name,
                domains=domains,
                max_results=max_results,
            )
            duration_ms = int((time.time() - start_time) * 1000)
            logger.log(
                section=section_name,
                provider_type="search",
                provider_name=provider_name,
                query_kind=section_name,
                attempt=1,
                duration_ms=duration_ms,
                status="success" if evidence else "empty",
                fallback_hop=fallback_hop,
            )
            if evidence:
                return evidence, attempted_providers, provider_failures
        except ProviderExecutionError as exc:
            duration_ms = int((time.time() - start_time) * 1000)
            provider_failures.append(
                _provider_failure(exc, section=section_name, fallback_hop=fallback_hop)
            )
            provider_health[health_key] = (
                "disabled_for_run" if _should_disable_for_run(exc.error_class) else "degraded"
            )
            logger.log(
                section=section_name,
                provider_type="search",
                provider_name=provider_name,
                query_kind=section_name,
                attempt=1,
                duration_ms=duration_ms,
                status="failure",
                http_status=exc.http_status,
                error_class=exc.error_class,
                error_message=exc.error_message,
                fallback_hop=fallback_hop,
            )

    return evidence, attempted_providers, provider_failures


def _synthesize_with_fallbacks(
    section_name: str,
    prompt: str,
    *,
    logger: JsonlLogger,
    provider_health: dict[str, str],
    force_failures: set[str],
    temperature: float = 0.3,
    max_tokens: int = 4096,
) -> tuple[str, list[str], list[ProviderFailure]]:
    attempted_profiles: list[str] = []
    provider_failures: list[ProviderFailure] = []

    for fallback_hop, profile_name in enumerate(SYNTHESIS_PROFILE_ORDER):
        health_key = f"synthesis:{profile_name}"
        if provider_health.get(health_key) == "disabled_for_run":
            logger.log(
                section=section_name,
                provider_type="synthesis",
                provider_name=profile_name,
                fallback_hop=fallback_hop,
                status="skipped_disabled",
            )
            continue

        attempted_profiles.append(profile_name)
        start_time = time.time()
        try:
            if _is_forced_failure(force_failures, "synthesis", profile_name):
                raise ProviderExecutionError(
                    "synthesis",
                    profile_name,
                    "forced_failure",
                    f"forced failure for {profile_name}",
                )
            gateway = LLMGateway(profile=profile_name)
            result = gateway.chat(
                messages=[{"role": "user", "content": prompt}],
                temperature=temperature,
                max_tokens=max_tokens,
            ).strip()
            if not result:
                raise ProviderExecutionError(
                    "synthesis",
                    profile_name,
                    "empty",
                    f"{profile_name} returned an empty response",
                )

            duration_ms = int((time.time() - start_time) * 1000)
            logger.log(
                section=section_name,
                provider_type="synthesis",
                provider_name=profile_name,
                attempt=1,
                duration_ms=duration_ms,
                status="success",
                fallback_hop=fallback_hop,
            )
            return result, attempted_profiles, provider_failures
        except ProviderExecutionError as exc:
            duration_ms = int((time.time() - start_time) * 1000)
            provider_failures.append(
                _provider_failure(exc, section=section_name, fallback_hop=fallback_hop)
            )
            provider_health[health_key] = (
                "disabled_for_run" if _should_disable_for_run(exc.error_class) else "degraded"
            )
            logger.log(
                section=section_name,
                provider_type="synthesis",
                provider_name=profile_name,
                attempt=1,
                duration_ms=duration_ms,
                status="failure",
                error_class=exc.error_class,
                error_message=exc.error_message,
                fallback_hop=fallback_hop,
            )
        except Exception as exc:  # pragma: no cover - runtime dependency
            duration_ms = int((time.time() - start_time) * 1000)
            error_class = _classify_error(str(exc))
            provider_failures.append(
                asdict(
                    ProviderFailure(
                        provider_type="synthesis",
                        provider_name=profile_name,
                        error_class=error_class,
                        error_message=str(exc),
                        section=section_name,
                        fallback_hop=fallback_hop,
                    )
                )
            )
            provider_health[health_key] = (
                "disabled_for_run" if _should_disable_for_run(error_class) else "degraded"
            )
            logger.log(
                section=section_name,
                provider_type="synthesis",
                provider_name=profile_name,
                attempt=1,
                duration_ms=duration_ms,
                status="failure",
                error_class=error_class,
                error_message=str(exc),
                fallback_hop=fallback_hop,
            )

    normalized_failures = [
        failure if isinstance(failure, dict) else asdict(failure)
        for failure in provider_failures
    ]
    return "", attempted_profiles, normalized_failures


def _company_queries(company_name: str, domain: str) -> list[str]:
    return [
        f'"{company_name}" company overview {domain}',
        f'"{company_name}" funding investors',
        f'"{company_name}" annual revenue employees',
        f'"{company_name}" customers case study',
        f'"{company_name}" product platform',
        f'"{company_name}" news 2025 2026',
    ]


def _contact_queries(company_name: str) -> list[str]:
    return [
        f'site:linkedin.com/in "{company_name}" CEO',
        f'site:linkedin.com/in "{company_name}" CMO',
        f'site:linkedin.com/in "{company_name}" CRO',
        f'site:linkedin.com/in "{company_name}" Founder',
        f'site:linkedin.com/in "{company_name}" "VP Marketing"',
        f'site:linkedin.com/in "{company_name}" "Director Marketing"',
        f'site:linkedin.com/in "{company_name}" "Demand Generation"',
        f'site:linkedin.com/in "{company_name}" ABM',
        f'site:linkedin.com/in "{company_name}" "Marketing Operations"',
        f'site:linkedin.com/in "{company_name}" "Revenue Operations"',
        f'site:linkedin.com/in "{company_name}" "VP Sales"',
    ]


def _news_queries(company_name: str, domain: str) -> list[str]:
    del domain
    return [
        f'"{company_name}" press release',
        f'"{company_name}" product launch',
        f'"{company_name}" partnership announcement',
        f'"{company_name}" executive hire',
        f'"{company_name}" funding round',
    ]


def _build_company_prompt(company_name: str, apollo_data: dict[str, Any], evidence: list[EvidenceItem]) -> str:
    apollo_block = json.dumps(
        {
            "name": apollo_data.get("name", company_name),
            "industry": apollo_data.get("industry", ""),
            "estimated_employees": apollo_data.get("estimated_employees"),
            "annual_revenue": apollo_data.get("annual_revenue"),
            "latest_funding_round_type": apollo_data.get("latest_funding_round_type"),
            "latest_funding_round_date": apollo_data.get("latest_funding_round_date"),
            "keywords": apollo_data.get("keywords", []),
        },
        indent=2,
    )
    return f"""# ROLE
Act as a Senior Investment Analyst specializing in venture-backed software companies.

# COMPANY
{company_name}

# APOLLO BASELINE
{apollo_block}

# RAW WEB EVIDENCE
{_evidence_to_block(evidence)}

# TASK
Use only the evidence above plus the Apollo baseline to produce a clean company deep dive.

# FORMAT
Plain text only. No markdown.

EXECUTIVE SUMMARY

[2-3 paragraphs]

1. SNAPSHOT AND MARKET PRESENCE

LinkedIn Employee Count: ...
Estimated Annual Revenue: ...
Estimated Company Value: ...
Status: ...

2. FUNDING AND CAPITAL STRUCTURE

Total Funding Raised: ...
Latest Funding Round: ...
Funding History:
...
Key Investors: ...

3. BUSINESS MODEL AND OPERATIONS

Revenue Model: ...
Key Customer Logos: ...
Operational Scale: ...

4. COMPANY NARRATIVE AND PRODUCT

History: ...
Product Overview: ...
Product Differentiation: ...

If a data point is uncertain, provide a bounded estimate and say what evidence supports it."""


def _build_techstack_prompt(company_name: str, apollo_tech: list[dict[str, Any]], scraped_tech: list[str]) -> str:
    all_tech = []
    for tech in apollo_tech:
        all_tech.append(f"{tech['name']} (Category: {tech['category']}) [Source: Apollo]")
    for tech in scraped_tech:
        all_tech.append(f"{tech} [Source: Website Scrape]")
    tech_data = "\n".join(all_tech) if all_tech else "No technologies detected"
    return f"""# ROLE
Act as a RevOps technologist analyzing the marketing and sales stack for {company_name}.

# RAW DATA
{tech_data}

# TASK
Identify only go-to-market tools. Ignore engineering, security, IT, and hosting products.

# FORMAT
Plain text only. No markdown.

CRM
...

Marketing Automation (MAP)
...

ABM & Intent
...

Sales Engagement (SEP)
...

Conversational / Chat
...

CMS / Web
...

Analytics & Attribution
...

Event / Webinar
...

Other RevTech
...

Only include categories where tools were found."""


def _extract_profiles_from_evidence(evidence: list[EvidenceItem]) -> dict[str, dict[str, str]]:
    profiles: dict[str, dict[str, str]] = {}
    for item in evidence:
        if "linkedin.com/in/" not in item.url:
            continue
        profiles[item.url] = {
            "url": item.url,
            "title": item.title,
            "snippet": item.snippet,
            "query": item.query,
        }
    return profiles


_COMPANY_SUFFIX_TOKENS = (
    "co",
    "co.",
    "company",
    "corp",
    "corp.",
    "corporation",
    "inc",
    "inc.",
    "llc",
    "ltd",
    "ltd.",
    "limited",
    "group",
    "holdings",
    "software",
    "systems",
    "technologies",
    "technology",
)


def _bucket_for_profile(profile: dict[str, str]) -> str:
    haystack = " ".join(
        [
            profile.get("title", ""),
            profile.get("snippet", ""),
            profile.get("query", ""),
        ]
    ).lower()
    marketing_tokens = (
        "marketing",
        "demand",
        "abm",
        "abx",
        "growth",
        "field marketing",
        "product marketing",
        "content marketing",
        "revops",
        "revenue operations",
    )
    if any(token in haystack for token in marketing_tokens):
        return "MARKETING"
    return "LEADERSHIP"


def _profile_explicitly_mentions_company(profile: dict[str, str], company_name: str) -> bool:
    title = profile.get("title", "")
    snippet = profile.get("snippet", "")
    search_text = re.sub(r"\s+", " ", f"{title} {snippet}".strip())
    normalized_company = re.sub(r"\s+", " ", company_name.strip())
    if not search_text or not normalized_company:
        return False

    lowered_title = re.sub(r"\s+", " ", title.strip()).lower()
    lowered_snippet = re.sub(r"\s+", " ", snippet.strip()).lower()
    lowered_text = search_text.lower()
    lowered_company = normalized_company.lower()
    if lowered_company not in lowered_text:
        return False

    company_tokens = lowered_company.split()
    if len(company_tokens) > 1:
        return True

    company_token = re.escape(company_tokens[0])
    suffix_pattern = "|".join(re.escape(token) for token in _COMPANY_SUFFIX_TOKENS)

    title_employer_cues = (
        rf"\s-\s{company_token}(?=\s+\|\slinkedin\b)",
        rf"\bat\s+{company_token}(?=\s*(?:[,|()/-]|$))",
        rf"\b{company_token}\s+(?:{suffix_pattern})\b",
    )
    snippet_employer_cues = (
        rf"\bexperience:\s*{company_token}(?=\s*(?:[,|()/-]|$))",
        rf"\bat\s+{company_token}(?=\s*(?:[,|()/-]|$))",
        rf"\b(?:cmo|ceo|cro|cfo|coo|cto|chief [a-z ]+ officer|director|vp|vice president|head|lead|manager)\b[^.]{0,80}\b{company_token}\b",
    )
    return any(re.search(pattern, lowered_title) for pattern in title_employer_cues) or any(
        re.search(pattern, lowered_snippet) for pattern in snippet_employer_cues
    )


def _build_contacts_prompt(company_name: str, profiles: dict[str, dict[str, str]]) -> str:
    lines: list[str] = []
    for profile in profiles.values():
        lines.extend(
            [
                f"Bucket: {_bucket_for_profile(profile)}",
                f"URL: {profile['url']}",
                f"Title/Name from Search: {profile.get('title', '')}",
                f"Snippet: {profile.get('snippet', '')}",
                f"Search Query Used: {profile.get('query', '')}",
                "---",
            ]
        )
    profiles_text = "\n".join(lines)
    return f"""# ROLE
Act as an Executive Sales Researcher formatting LinkedIn contact data for {company_name}.

# RAW DATA
{profiles_text}

# TASK
Output two sections in this exact order:

Marketing
Then a divider line:
--- LEADERSHIP ---
Then Leadership

# FORMAT
Plain text only. No markdown.

CONTACT NAME
Title: [Current title at {company_name}]
LinkedIn: [full linkedin.com/in URL]
Location: [City, State/Country or "Verify on profile"]
Insight: [brief background note]

Use one blank line between contacts. Only include people who appear to work at {company_name}.
Exclude people who merely mention Asana as a tool, skill, client, or adjacent brand name."""


def _build_news_prompt(company_name: str, evidence: list[EvidenceItem]) -> str:
    return f"""# ROLE
You are a business research analyst summarizing recent news about {company_name}.

# RAW NEWS DATA
{_evidence_to_block(evidence)}

# TASK
Summarize the most relevant 5-10 items in plain text.

# FORMAT
[Date or "Recent"] - [Headline]
Source: [Publication Name]
Summary: [2-3 sentences]

Use a blank line between items. Focus on funding, partnerships, products, and executive changes."""


def _build_champion_text(
    champion_name: str,
    company_name: str,
    *,
    logger: JsonlLogger,
    provider_health: dict[str, str],
    force_failures: set[str],
) -> tuple[str | None, str | None]:
    config = get_config()
    if not config.get("gemini_api_key"):
        return None, None

    champion_text, champion_url = search_champion_contact(champion_name, company_name)
    if champion_text:
        logger.log(
            section="contacts",
            provider_type="search",
            provider_name="gemini_optional_champion",
            status="success",
        )
        return champion_text, champion_url

    logger.log(
        section="contacts",
        provider_type="search",
        provider_name="gemini_optional_champion",
        status="empty",
    )
    return None, None


def _run_company_section(
    company_name: str,
    domain: str,
    apollo_data: dict[str, Any],
    *,
    logger: JsonlLogger,
    provider_health: dict[str, str],
    force_failures: set[str],
) -> SectionResult:
    evidence, provider_chain, provider_failures = _search_with_fallbacks(
        "company_research",
        _company_queries(company_name, domain),
        logger=logger,
        provider_health=provider_health,
        force_failures=force_failures,
        max_results=4,
    )
    if not evidence:
        status = "provider_failed" if provider_failures else "no_results_valid"
        return SectionResult(
            name="company_research",
            status=status,
            raw_evidence=[asdict(item) for item in evidence],
            provider_chain=provider_chain,
            provider_failures=[asdict(failure) for failure in provider_failures],
            notes="No viable search evidence gathered for company research.",
        )

    content, synth_chain, synth_failures = _synthesize_with_fallbacks(
        "company_research",
        _build_company_prompt(company_name, apollo_data, evidence),
        logger=logger,
        provider_health=provider_health,
        force_failures=force_failures,
        temperature=0.3,
        max_tokens=8192,
    )
    all_failures = [asdict(failure) for failure in provider_failures]
    all_failures.extend(synth_failures)
    return SectionResult(
        name="company_research",
        status="ok" if content else "synthesis_failed",
        content=content,
        raw_evidence=[asdict(item) for item in evidence],
        provider_chain=provider_chain + synth_chain,
        provider_failures=all_failures,
    )


def _run_techstack_section(
    company_name: str,
    apollo_data: dict[str, Any],
    scraped_tech: list[str],
    *,
    logger: JsonlLogger,
    provider_health: dict[str, str],
    force_failures: set[str],
) -> SectionResult:
    apollo_tech = apollo_data.get("tech_stack", [])
    raw_evidence = [{"apollo_tech": apollo_tech, "scraped_tech": scraped_tech}]
    if not apollo_tech and not scraped_tech:
        logger.log(section="techstack", provider_type="data", provider_name="apollo_scrape", status="empty")
        return SectionResult(
            name="techstack",
            status="no_results_valid",
            raw_evidence=raw_evidence,
            notes="Apollo and site scraping returned no tech signals.",
        )

    content, synth_chain, synth_failures = _synthesize_with_fallbacks(
        "techstack",
        _build_techstack_prompt(company_name, apollo_tech, scraped_tech),
        logger=logger,
        provider_health=provider_health,
        force_failures=force_failures,
        temperature=0.2,
        max_tokens=2048,
    )
    return SectionResult(
        name="techstack",
        status="ok" if content else "synthesis_failed",
        content=content,
        raw_evidence=raw_evidence,
        provider_chain=synth_chain,
        provider_failures=synth_failures,
    )


def _run_contacts_section(
    company_name: str,
    champion_name: str | None,
    *,
    logger: JsonlLogger,
    provider_health: dict[str, str],
    force_failures: set[str],
) -> SectionResult:
    evidence, provider_chain, provider_failures = _search_with_fallbacks(
        "contacts",
        _contact_queries(company_name),
        logger=logger,
        provider_health=provider_health,
        force_failures=force_failures,
        domains=["linkedin.com"],
        max_results=4,
    )
    profiles = _extract_profiles_from_evidence(evidence)
    profiles = {
        url: profile
        for url, profile in profiles.items()
        if _profile_explicitly_mentions_company(profile, company_name)
    }
    profiles = validate_and_fix_linkedin_urls(profiles, company_name)
    if len(profiles) < 3:
        status = "provider_failed" if provider_failures and not profiles else "no_results_valid"
        return SectionResult(
            name="contacts",
            status=status,
            raw_evidence=[asdict(item) for item in evidence],
            provider_chain=provider_chain,
            provider_failures=[asdict(failure) for failure in provider_failures],
            notes=f"Only {len(profiles)} valid LinkedIn profiles matched explicit employer cues.",
        )

    content, synth_chain, synth_failures = _synthesize_with_fallbacks(
        "contacts",
        _build_contacts_prompt(company_name, profiles),
        logger=logger,
        provider_health=provider_health,
        force_failures=force_failures,
        temperature=0.2,
        max_tokens=4096,
    )
    if not content:
        return SectionResult(
            name="contacts",
            status="synthesis_failed",
            raw_evidence=[asdict(item) for item in evidence],
            provider_chain=provider_chain + synth_chain,
            provider_failures=[asdict(failure) for failure in provider_failures] + synth_failures,
        )

    champion_text = None
    champion_url = None
    if champion_name:
        champion_text, champion_url = _build_champion_text(
            champion_name,
            company_name,
            logger=logger,
            provider_health=provider_health,
            force_failures=force_failures,
        )
        if champion_text:
            if "Insight:" not in champion_text:
                champion_text += "\nInsight: Deal champion for this opportunity"
            else:
                champion_text = champion_text.replace(
                    "Insight:",
                    "Insight: Deal champion for this opportunity. ",
                    1,
                )
            content = deduplicate_champion_from_contacts(content, champion_url, champion_name)
            content = f"CHAMPION\n{champion_text}\n\n{content}"

    content, url_mappings = extract_and_strip_linkedin_lines(content)
    if champion_name and not champion_text:
        placeholder = (
            f"CHAMPION\n{champion_name}\nTitle: Verify on profile\n"
            f"LinkedIn: Search manually\nInsight: Deal champion for this opportunity\n\n"
        )
        content = placeholder + content

    return SectionResult(
        name="contacts",
        status="ok",
        content=content,
        raw_evidence=[asdict(item) for item in evidence],
        provider_chain=provider_chain + synth_chain,
        provider_failures=[asdict(failure) for failure in provider_failures] + synth_failures,
        url_mappings=url_mappings,
    )


def _run_news_section(
    company_name: str,
    domain: str,
    *,
    logger: JsonlLogger,
    provider_health: dict[str, str],
    force_failures: set[str],
) -> SectionResult:
    evidence, provider_chain, provider_failures = _search_with_fallbacks(
        "news",
        _news_queries(company_name, domain),
        logger=logger,
        provider_health=provider_health,
        force_failures=force_failures,
        max_results=4,
    )
    if not evidence:
        return SectionResult(
            name="news",
            status="no_results_valid" if not provider_failures else "provider_failed",
            raw_evidence=[asdict(item) for item in evidence],
            provider_chain=provider_chain,
            provider_failures=[asdict(failure) for failure in provider_failures],
            notes="No recent news evidence was gathered.",
        )

    content, synth_chain, synth_failures = _synthesize_with_fallbacks(
        "news",
        _build_news_prompt(company_name, evidence),
        logger=logger,
        provider_health=provider_health,
        force_failures=force_failures,
        temperature=0.2,
        max_tokens=3072,
    )
    status = "ok" if content else "synthesis_failed"
    return SectionResult(
        name="news",
        status=status,
        content=content,
        raw_evidence=[asdict(item) for item in evidence],
        provider_chain=provider_chain + synth_chain,
        provider_failures=[asdict(failure) for failure in provider_failures] + synth_failures,
    )


def _evaluate_run_status(sections: dict[str, SectionResult]) -> tuple[str, list[str], list[str], str]:
    failed_sections = [
        name for name in REQUIRED_SECTION_NAMES if sections[name].status != "ok"
    ]
    degraded_sections = [
        name
        for name in OPTIONAL_SECTION_NAMES
        if sections[name].status not in {"ok", "no_results_valid"}
    ]
    if failed_sections:
        return "blocked", failed_sections, degraded_sections, "quality gate failed"
    return "success", failed_sections, degraded_sections, ""


def _serializable_run_result(result: ResearchRunResult) -> dict[str, Any]:
    payload = asdict(result)
    payload["sections"] = {
        name: asdict(section) if isinstance(section, SectionResult) else section
        for name, section in result.sections.items()
    }
    return payload


def run_research(
    company_name: str,
    domain: str,
    champion_name: str | None = None,
    *,
    skip_browser: bool = False,
    force_failures: set[str] | None = None,
    smoke_test: bool = False,
) -> ResearchRunResult:
    force_failures = force_failures or set()
    run_id = os.environ.get("DEAL_RESEARCH_RUN_ID", f"deal-{uuid.uuid4().hex[:12]}")
    logger = JsonlLogger(run_id, company_name, domain)
    provider_health: dict[str, str] = {}
    created_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"\n{'='*60}")
    print("Deal Research Generator")
    print(f"{'='*60}")
    print(f"Run ID: {run_id}")
    print(f"Company: {company_name}")
    print(f"Domain: {domain}")
    if champion_name:
        print(f"Champion: {champion_name}")
    print(f"{'='*60}\n")

    logger.log(section="run", provider_type="workflow", provider_name="deal_research", status="started")

    print("[Step 1/6] Fetching company data from Apollo API...")
    apollo_data = fetch_apollo_data(domain) or {
        "name": company_name,
        "domain": domain,
        "tech_stack": [],
    }
    if not apollo_data.get("industry"):
        print("  Warning: Apollo enrichment returned partial data")

    print("\n[Step 2/6] Scanning website for tech stack...")
    scraped_tech = scrape_website_tech_stack(domain)

    print("\n[Step 3/6] Generating company research...")
    company_section = _run_company_section(
        company_name,
        domain,
        apollo_data,
        logger=logger,
        provider_health=provider_health,
        force_failures=force_failures,
    )

    print("\n[Step 4/6] Generating tech stack analysis...")
    techstack_section = _run_techstack_section(
        company_name,
        apollo_data,
        scraped_tech,
        logger=logger,
        provider_health=provider_health,
        force_failures=force_failures,
    )

    print("\n[Step 5/6] Finding contacts...")
    contacts_section = _run_contacts_section(
        company_name,
        champion_name,
        logger=logger,
        provider_health=provider_health,
        force_failures=force_failures,
    )

    print("\n[Step 6/6] Gathering news and activity...")
    news_section = _run_news_section(
        company_name,
        domain,
        logger=logger,
        provider_health=provider_health,
        force_failures=force_failures,
    )

    sections = {
        "company_research": company_section,
        "techstack": techstack_section,
        "contacts": contacts_section,
        "news": news_section,
    }
    status, failed_sections, degraded_sections, blocked_reason = _evaluate_run_status(sections)

    doc_created = False
    doc_url = ""
    if status == "success" and not smoke_test:
        news_text = ""
        if news_section.status == "ok":
            news_text = news_section.content
        elif news_section.status == "no_results_valid":
            news_text = "No recent news found from the configured sources."
        doc_url = create_google_doc(
            company_name,
            company_section.content,
            techstack_section.content,
            contacts_section.content,
            news_text,
            url_mappings=contacts_section.url_mappings,
        )
        doc_created = bool(doc_url)
        if doc_url and not skip_browser and not os.environ.get("SKIP_BROWSER"):
            webbrowser.open(doc_url)
    elif smoke_test:
        status = "smoke_test"
        blocked_reason = blocked_reason or "smoke test mode"

    provider_failures: list[dict[str, Any]] = []
    for section in sections.values():
        provider_failures.extend(section.provider_failures)

    result = ResearchRunResult(
        run_id=run_id,
        company_name=company_name,
        domain=domain,
        champion_name=champion_name,
        status=status,
        doc_created=doc_created,
        doc_url=doc_url,
        blocked_reason=blocked_reason,
        failed_sections=failed_sections,
        degraded_sections=degraded_sections,
        provider_failures=provider_failures,
        sections=sections,
        jsonl_log_path=str(logger.path),
        created_at=created_at,
    )
    logger.log(
        section="run",
        provider_type="workflow",
        provider_name="deal_research",
        status=status,
        failed_sections=failed_sections,
        degraded_sections=degraded_sections,
        doc_created=doc_created,
        doc_url=doc_url,
    )
    return result


def run_doctor(*, json_output: bool = False) -> int:
    config = get_config()
    checks: list[dict[str, str]] = []
    ok = True

    def record(name: str, status: str, message: str) -> None:
        nonlocal ok
        if status != "ok":
            ok = False
        checks.append({"name": name, "status": status, "message": message})

    for env_name in ("APOLLO_API_KEY", "GOOGLE_DRIVE_FOLDER_ID", "TAVILY_API_KEY", "BRAVE_API_KEY", "PERPLEXITY_API_KEY", "AI_DEEPSEEK_KEY", "AI_OPENAI_KEY"):
        record(
            env_name,
            "ok" if os.environ.get(env_name) else "fail",
            "present" if os.environ.get(env_name) else "missing",
        )

    try:
        docs_service = build_service("docs", "v1")
        docs_service.documents().get(documentId="does-not-exist").execute()
    except Exception as exc:
        if "Requested entity was not found" in str(exc):
            record("google_docs_auth", "ok", "Google Docs auth works")
        else:
            record("google_docs_auth", "fail", str(exc))

    try:
        drive_service = build_service("drive", "v3")
        drive_service.files().get(
            fileId=config["google_drive_folder_id"],
            fields="id,name",
            supportsAllDrives=True,
        ).execute()
        record("google_drive_folder", "ok", "Drive folder reachable")
    except Exception as exc:
        record("google_drive_folder", "fail", str(exc))

    probe_query = ["Acme software company"]
    for provider_name in SEARCH_PROVIDER_ORDER:
        try:
            SEARCH_PROVIDER_EXECUTORS[provider_name](
                probe_query,
                surface="doctor",
                max_results=1,
            )
            record(f"search_provider:{provider_name}", "ok", "probe succeeded")
        except Exception as exc:
            record(f"search_provider:{provider_name}", "fail", str(exc))

    for profile_name in SYNTHESIS_PROFILE_ORDER:
        try:
            gateway = LLMGateway(profile=profile_name)
            gateway.chat(
                messages=[{"role": "user", "content": "Reply with OK"}],
                temperature=0,
                max_tokens=8,
            )
            record(f"synthesis_profile:{profile_name}", "ok", "probe succeeded")
        except Exception as exc:
            record(f"synthesis_profile:{profile_name}", "fail", str(exc))

    payload = {
        "ok": ok,
        "checks": checks,
        "checked_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if json_output:
        print(json.dumps(payload, sort_keys=True))
    else:
        for check in checks:
            print(f"[{check['status'].upper()}] {check['name']}: {check['message']}")
    return 0 if ok else 1


def _parse_args(argv: list[str]) -> argparse.Namespace:
    if argv and argv[0] == "doctor":
        parser = argparse.ArgumentParser(prog="deal_research.py doctor")
        parser.add_argument("command")
        parser.add_argument("--json", action="store_true")
        return parser.parse_args(argv)

    parser = argparse.ArgumentParser(
        prog="deal_research.py",
        description="Generate a deal research document with explicit quality gating.",
    )
    parser.add_argument("company_name")
    parser.add_argument("domain")
    parser.add_argument("champion_name", nargs="?")
    parser.add_argument("--json", action="store_true", help="Emit a machine-readable result line at the end.")
    parser.add_argument("--no-browser", action="store_true", help="Do not open the final Google Doc in a browser.")
    parser.add_argument(
        "--force-failure",
        action="append",
        default=[],
        help="Simulate a provider failure by name, e.g. tavily or synthesis:workhorse.",
    )
    parser.add_argument("--smoke-test", action="store_true", help="Run the workflow without creating the final doc.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv or sys.argv[1:])
    if getattr(args, "command", None) == "doctor":
        return run_doctor(json_output=args.json)

    result = run_research(
        args.company_name,
        args.domain,
        args.champion_name,
        skip_browser=args.no_browser or args.json,
        force_failures=_normalize_force_failures(args.force_failure),
        smoke_test=args.smoke_test,
    )

    print(f"\n{'='*60}")
    print("RUN SUMMARY")
    print(f"{'='*60}")
    print(f"Run ID: {result.run_id}")
    print(f"Status: {result.status}")
    if result.failed_sections:
        print(f"Failed Sections: {', '.join(result.failed_sections)}")
    if result.degraded_sections:
        print(f"Degraded Sections: {', '.join(result.degraded_sections)}")
    if result.doc_url:
        print(f"Google Doc: {result.doc_url}")
    if result.blocked_reason:
        print(f"Blocked Reason: {result.blocked_reason}")
    print(f"JSONL Log: {result.jsonl_log_path}")
    print(f"{'='*60}\n")

    if args.json:
        print(f"{JSON_RESULT_PREFIX} {json.dumps(_serializable_run_result(result), sort_keys=True)}")

    if result.status == "error":
        return 1
    if result.status == "blocked":
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
