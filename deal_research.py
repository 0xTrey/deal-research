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

import json
import os
import re
import sys
import time
import webbrowser
from datetime import datetime
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from llm_gateway import LLMGateway
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

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

    # Support AI_GEMINI_KEY (gateway convention) as fallback for GEMINI_API_KEY
    if not os.environ.get("GEMINI_API_KEY") and os.environ.get("AI_GEMINI_KEY"):
        os.environ["GEMINI_API_KEY"] = os.environ["AI_GEMINI_KEY"]

    required = ["APOLLO_API_KEY", "GEMINI_API_KEY", "GOOGLE_DRIVE_FOLDER_ID"]
    missing = [key for key in required if not os.environ.get(key)]
    if missing:
        print(f"ERROR: Missing required environment variables: {', '.join(missing)}")
        print("Please copy .env.example to .env and fill in your API keys.")
        sys.exit(1)

    # Tavily is optional - will fall back to Gemini if not configured
    tavily_key = os.environ.get("TAVILY_API_KEY")
    if not tavily_key:
        print("  Note: TAVILY_API_KEY not set - will use Gemini for LinkedIn search")

    return {
        "apollo_api_key": os.environ["APOLLO_API_KEY"],
        "gemini_api_key": os.environ["GEMINI_API_KEY"],
        "tavily_api_key": tavily_key,
        "google_drive_folder_id": os.environ["GOOGLE_DRIVE_FOLDER_ID"],
        "google_credentials_path": os.path.expanduser(
            os.environ.get("GOOGLE_CREDENTIALS_PATH", "~/.config/deal-research/credentials.json")
        ),
        "google_token_path": os.path.expanduser(
            os.environ.get("GOOGLE_TOKEN_PATH", "~/.config/deal-research/token.json")
        ),
    }

# Global config - loaded at runtime
CONFIG = None

def get_config():
    """Get or initialize configuration."""
    global CONFIG
    if CONFIG is None:
        CONFIG = load_config()
    return CONFIG

# Google API scopes
SCOPES = [
    "https://www.googleapis.com/auth/documents",
    "https://www.googleapis.com/auth/drive",
]

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
    """Native Gemini REST call for google_search grounding (not OpenAI-compatible)."""
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

    try:
        response = requests.post(
            url, headers={"Content-Type": "application/json"}, json=payload, timeout=180
        )
        response.raise_for_status()
        data = response.json()

        candidates = data.get("candidates", [])
        if candidates:
            parts = candidates[0].get("content", {}).get("parts", [])
            return "\n".join(p["text"] for p in parts if "text" in p)

        return ""
    except Exception as e:
        print(f"  [Gemini/Grounded] Error: {e}")
        return ""


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

    # Use Google Search grounding to get real-time data
    return call_gemini_api(prompt, use_search=True)


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

def get_google_credentials():
    """Get or refresh Google API credentials using OAuth."""
    creds = None
    config = get_config()
    token_path = config["google_token_path"]
    credentials_path = config["google_credentials_path"]

    # Load existing token if available
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    # Refresh or get new credentials if needed
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("  [Google] Refreshing access token...")
            creds.refresh(Request())
        else:
            if not os.path.exists(credentials_path):
                print(f"\n  ERROR: {credentials_path} not found!")
                print("  Please create credentials.json with your OAuth client configuration.")
                print("  See setup instructions in the script comments.")
                return None
            print("  [Google] Starting OAuth flow...")
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save the token for future use
        os.makedirs(os.path.dirname(token_path), exist_ok=True)
        with open(token_path, "w") as token:
            token.write(creds.to_json())

    return creds


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

    creds = get_google_credentials()
    if not creds:
        return None

    # Build services
    docs_service = build("docs", "v1", credentials=creds)
    drive_service = build("drive", "v3", credentials=creds)

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
# MAIN ORCHESTRATION
# =============================================================================

def main():
    """Main function to orchestrate deal research generation."""
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: python deal_research.py \"Company Name\" \"domain.com\" [\"Champion Name\"]")
        print("Example: python deal_research.py \"Asana\" \"asana.com\"")
        print("Example: python deal_research.py \"iManage\" \"imanage.com\" \"Rajiv Chidambaram\"")
        sys.exit(1)

    company_name = sys.argv[1]
    domain = sys.argv[2]
    champion_name = sys.argv[3] if len(sys.argv) == 4 else None

    print(f"\n{'='*60}")
    print(f"Deal Research Generator")
    print(f"{'='*60}")
    print(f"Company: {company_name}")
    print(f"Domain: {domain}")
    if champion_name:
        print(f"Champion: {champion_name}")
    print(f"{'='*60}\n")

    # Step 1: Fetch Apollo data
    print("[Step 1/6] Fetching company data from Apollo API...")
    apollo_data = fetch_apollo_data(domain)
    if not apollo_data:
        apollo_data = {
            "name": company_name,
            "domain": domain,
            "tech_stack": []
        }
        print("  Warning: Using minimal data (Apollo enrichment failed)")

    # Step 2: Scrape website for tech stack
    print("\n[Step 2/6] Scanning website for tech stack...")
    scraped_tech = scrape_website_tech_stack(domain)

    # Step 3: Generate LLM-synthesized sections
    print("\n[Step 3/6] Generating research sections with Gemini...")

    print("\n  Generating Company Research...")
    company_research = generate_company_research(apollo_data, company_name)
    if not company_research:
        company_research = "Error generating company research. Please add manually."

    print("\n  Generating TechStack analysis...")
    techstack = generate_techstack_analysis(
        apollo_data.get("tech_stack", []),
        scraped_tech,
        company_name
    )
    if not techstack:
        techstack = "Error generating tech stack analysis. Please add manually."

    # Step 4: Deep research for LinkedIn contacts using Tavily + Gemini
    print("\n[Step 4/6] Searching for LinkedIn contacts...")
    contacts = search_linkedin_contacts_with_tavily(company_name)
    if not contacts:
        contacts = "Error finding contacts. Please add manually."

    # Champion search: find the champion, deduplicate, prepend to contacts.
    # The champion is ALWAYS placed at the top of the Contacts section,
    # even if the LinkedIn search doesn't find a profile URL.
    if champion_name:
        champion_text, champion_url = search_champion_contact(champion_name, company_name)
        if champion_text:
            # Inject champion insight if not already present
            if "Insight:" not in champion_text:
                champion_text += f"\nInsight: Deal champion for this opportunity"
            else:
                # Append champion note to existing insight
                champion_text = champion_text.replace(
                    "Insight:", "Insight: Deal champion for this opportunity."
                )
            contacts = deduplicate_champion_from_contacts(contacts, champion_url, champion_name)
            contacts = f"CHAMPION\n{champion_text}\n\n{contacts}"
        else:
            # LinkedIn search failed -- still add the champion as a placeholder
            contacts = deduplicate_champion_from_contacts(contacts, None, champion_name)
            champion_placeholder = (
                f"{champion_name}\n"
                f"Title: Verify on profile\n"
                f"LinkedIn: Search manually\n"
                f"Insight: Deal champion for this opportunity"
            )
            contacts = f"CHAMPION\n{champion_placeholder}\n\n{contacts}"

    # Extract LinkedIn URLs and remove "LinkedIn:" lines from contacts text
    contacts, url_mappings = extract_and_strip_linkedin_lines(contacts)

    # Step 5: Gather recent news and activity using Tavily
    print("\n[Step 5/6] Gathering recent news and activity...")
    news_and_activity = generate_news_and_activity(company_name, domain)
    if not news_and_activity:
        news_and_activity = "Error gathering news. Please add manually."

    # Step 6: Create Google Doc
    doc_url = create_google_doc(company_name, company_research, techstack, contacts, news_and_activity, url_mappings=url_mappings)

    # Summary
    print(f"\n{'='*60}")
    print("COMPLETE!")
    print(f"{'='*60}")
    if doc_url:
        print(f"\nGoogle Doc: {doc_url}")
        print("  Opening in browser...")
        webbrowser.open(doc_url)
    print(f"\nData collected:")
    print(f"  - Apollo enrichment: {'Success' if apollo_data.get('industry') else 'Partial'}")
    print(f"  - Tech stack detected: {len(scraped_tech)} technologies from website")
    print(f"  - LinkedIn contacts: Tavily + Gemini search completed")
    print(f"  - News & Activity: Tavily search completed")
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
