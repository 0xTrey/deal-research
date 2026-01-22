#!/usr/bin/env python3
"""
Deal Research Generator - Creates pre-filled Google Docs for sales deal research.

Usage: python deal_research.py "Company Name" "domain.com"

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
from datetime import datetime
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
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
    config = get_config()

    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={config['gemini_api_key']}"

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

    headers = {"Content-Type": "application/json"}

    payload = {
        "contents": [{
            "parts": [{"text": prompt}]
        }],
        "generationConfig": {
            "maxOutputTokens": 8192,
            "temperature": 0.7,
        },
        "tools": [{
            "google_search": {}
        }]
    }

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        data = response.json()

        # Extract generated text from all parts
        candidates = data.get("candidates", [])
        if candidates:
            content = candidates[0].get("content", {})
            parts = content.get("parts", [])

            # Combine text from all parts
            result_parts = []
            for part in parts:
                if "text" in part:
                    result_parts.append(part["text"])

            result = "\n".join(result_parts)

            # Count approximate contacts found (look for common LinkedIn patterns)
            contact_count = result.lower().count("linkedin.com/in/") + result.lower().count("linkedin profile")
            # Also count by looking for name patterns with titles
            name_count = len([line for line in result.split('\n') if '**Name:**' in line or '**Title:**' in line])
            contact_estimate = max(contact_count, name_count // 2)

            print(f"  [Gemini Deep Research] Found ~{contact_estimate} contacts")
            return result

        return ""
    except Exception as e:
        print(f"  [Gemini Deep Research] Error: {e}")
        return ""


def search_linkedin_contacts_with_tavily(company_name):
    """
    Use Tavily API + Gemini to find and format LinkedIn contacts.

    Two-pass approach:
    1. Pass 1: Tavily searches LinkedIn for profile URLs (10-15 targeted queries)
    2. Pass 2: Gemini formats the raw results into structured contact entries

    Returns the same plain text format as the original Gemini-only function.
    Falls back to Gemini Google Search if Tavily fails or returns few results.
    """
    config = get_config()

    # Check if Tavily is available and configured
    if not TAVILY_AVAILABLE or not config.get("tavily_api_key"):
        print("  [Tavily] Not available, falling back to Gemini Google Search...")
        return search_linkedin_contacts_with_gemini(company_name)

    print(f"  [Tavily] Searching for LinkedIn contacts at {company_name}...")

    try:
        tavily = TavilyClient(api_key=config["tavily_api_key"])

        # Define targeted search queries for different roles
        # Each query targets specific executive/marketing roles
        search_queries = [
            f'site:linkedin.com/in "{company_name}" CEO',
            f'site:linkedin.com/in "{company_name}" CMO',
            f'site:linkedin.com/in "{company_name}" CRO',
            f'site:linkedin.com/in "{company_name}" CFO',
            f'site:linkedin.com/in "{company_name}" COO',
            f'site:linkedin.com/in "{company_name}" Founder',
            f'site:linkedin.com/in "{company_name}" "VP Marketing"',
            f'site:linkedin.com/in "{company_name}" "Vice President Marketing"',
            f'site:linkedin.com/in "{company_name}" "Director Marketing"',
            f'site:linkedin.com/in "{company_name}" "Demand Generation"',
            f'site:linkedin.com/in "{company_name}" "ABM"',
            f'site:linkedin.com/in "{company_name}" "Marketing Operations"',
            f'site:linkedin.com/in "{company_name}" "Digital Marketing"',
            f'site:linkedin.com/in "{company_name}" "Growth Marketing"',
        ]

        # Collect all unique LinkedIn profiles from searches
        all_profiles = {}  # URL -> profile data (deduplication)

        for i, query in enumerate(search_queries):
            try:
                print(f"    [{i+1}/{len(search_queries)}] Searching: {query[:60]}...")

                response = tavily.search(
                    query=query,
                    search_depth="advanced",
                    include_domains=["linkedin.com"],
                    max_results=5
                )

                results = response.get("results", [])
                for result in results:
                    url = result.get("url", "")
                    # Only include linkedin.com/in/ profiles (not company pages)
                    if "linkedin.com/in/" in url and url not in all_profiles:
                        all_profiles[url] = {
                            "url": url,
                            "title": result.get("title", ""),
                            "snippet": result.get("content", ""),
                            "query": query
                        }

                # Brief pause to avoid rate limiting
                time.sleep(0.3)

            except Exception as e:
                print(f"    Warning: Query failed - {e}")
                continue

        print(f"  [Tavily] Found {len(all_profiles)} unique LinkedIn profiles")

        # Check if we got enough results
        if len(all_profiles) < 5:
            print("  [Tavily] Too few results, falling back to Gemini Google Search...")
            return search_linkedin_contacts_with_gemini(company_name)

        # Pass 2: Use Gemini to format the raw profile data
        print("  [Gemini] Formatting contact information...")

        # Prepare profile data for Gemini
        profiles_text = ""
        for url, data in list(all_profiles.items())[:25]:  # Limit to 25 profiles
            profiles_text += f"""
URL: {data['url']}
Title/Name from Search: {data['title']}
Snippet: {data['snippet']}
Search Query Used: {data['query']}
---
"""

        # Gemini formatting prompt - emphasizes exact output format
        format_prompt = f"""# ROLE
Act as an Executive Sales Researcher. Your task is to format LinkedIn contact data into a structured list.

# RAW DATA
Below are LinkedIn profile URLs and snippets found for people at {company_name}:

{profiles_text}

# TASK
Using ONLY the data provided above, format each person into the contact format below.
Focus on these roles in priority order:
1. C-Suite (CEO, CMO, CRO, CFO, COO) and Founders
2. VPs and Directors in Marketing, Sales, or Revenue
3. Anyone with ABM, Demand Generation, or Marketing Operations in their title

# CRITICAL FORMATTING INSTRUCTIONS
This output will be pasted into Google Docs which does NOT render markdown.
DO NOT use any markdown syntax (no **, no *, no #, no [], no ()).

Use this EXACT plain text format for each contact:

CONTACT NAME
Title: [Their Current Title at {company_name}]
LinkedIn: [Full URL exactly as provided]
Tenure: [Time at company if mentioned in snippet, otherwise "Verify on profile"]
Location: [City, State/Country if mentioned, otherwise "Verify on profile"]
Insight: [Brief note from the snippet about their background or expertise]

(blank line between contacts)

# RULES
1. Only include people who appear to CURRENTLY work at {company_name}
2. Use the exact LinkedIn URL provided - do not modify it
3. Extract name from the search title (usually "Name - Title | LinkedIn")
4. If information is not available in the snippet, write "Verify on profile"
5. Order contacts by seniority (C-Suite first, then VPs, then Directors, then others)
6. Aim to include 10-20 contacts if data is available

Now format the contacts:"""

        # Call Gemini API for formatting
        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={config['gemini_api_key']}"

        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [{"parts": [{"text": format_prompt}]}],
            "generationConfig": {
                "maxOutputTokens": 8192,
                "temperature": 0.3,  # Lower temp for more consistent formatting
            }
        }

        response = requests.post(url, headers=headers, json=payload, timeout=120)
        response.raise_for_status()
        data = response.json()

        # Extract formatted text
        candidates = data.get("candidates", [])
        if candidates:
            content = candidates[0].get("content", {})
            parts = content.get("parts", [])
            result_parts = []
            for part in parts:
                if "text" in part:
                    result_parts.append(part["text"])
            result = "\n".join(result_parts)

            # Count contacts in result
            contact_count = result.lower().count("linkedin.com/in/")
            print(f"  [Tavily+Gemini] Formatted {contact_count} contacts")
            return result

        print("  [Tavily] Formatting failed, falling back to Gemini Google Search...")
        return search_linkedin_contacts_with_gemini(company_name)

    except Exception as e:
        print(f"  [Tavily] Error: {e}")
        print("  [Tavily] Falling back to Gemini Google Search...")
        return search_linkedin_contacts_with_gemini(company_name)


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

        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={config['gemini_api_key']}"

        headers = {"Content-Type": "application/json"}
        payload = {
            "contents": [{"parts": [{"text": format_prompt}]}],
            "generationConfig": {
                "maxOutputTokens": 4096,
                "temperature": 0.3,
            }
        }

        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json()

        candidates = data.get("candidates", [])
        if candidates:
            content = candidates[0].get("content", {})
            parts = content.get("parts", [])
            result_parts = []
            for part in parts:
                if "text" in part:
                    result_parts.append(part["text"])
            result = "\n".join(result_parts)

            news_count = result.count("Source:")
            print(f"  [News] Formatted {news_count} news items")
            return result

        return "Error formatting news. Raw data was collected but formatting failed."

    except Exception as e:
        print(f"  [News] Error: {e}")
        return f"Error fetching news: {str(e)}"


# =============================================================================
# GEMINI API INTEGRATION
# =============================================================================

def call_gemini_api(prompt, max_tokens=16384, use_search=False):
    """Call Gemini API for text generation with optional Google Search grounding."""
    config = get_config()
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={config['gemini_api_key']}"

    headers = {
        "Content-Type": "application/json",
    }

    payload = {
        "contents": [{
            "parts": [{
                "text": prompt
            }]
        }],
        "generationConfig": {
            "maxOutputTokens": max_tokens,
            "temperature": 0.4,  # Lower temperature for more factual output
        }
    }

    # Add Google Search grounding if requested
    if use_search:
        payload["tools"] = [{"google_search": {}}]

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=180)
        response.raise_for_status()
        data = response.json()

        # Extract generated text from all parts
        candidates = data.get("candidates", [])
        if candidates:
            content = candidates[0].get("content", {})
            parts = content.get("parts", [])

            # Combine text from all parts (grounded responses may have multiple)
            result_parts = []
            for part in parts:
                if "text" in part:
                    result_parts.append(part["text"])

            return "\n".join(result_parts)

        return ""
    except Exception as e:
        print(f"  [Gemini] Error: {e}")
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


def apply_text_formatting(docs_service, doc_id, full_text):
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
        "Title:",
        "LinkedIn:",
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

    # Find LinkedIn URLs and create hyperlinks for contact names
    # Pattern: "Name\nTitle:" preceded by a blank line, with LinkedIn URL following
    lines = full_text.split('\n')
    current_pos = 0

    for i, line in enumerate(lines):
        line_start = current_pos
        line_end = current_pos + len(line)

        # Check if this line starts with "LinkedIn: http"
        if line.strip().startswith("LinkedIn:") and "linkedin.com" in line:
            # Extract the URL
            url_start = line.find("http")
            if url_start != -1:
                url = line[url_start:].strip()

                # Look back to find the contact name (should be 1-3 lines before)
                for j in range(1, 4):
                    if i - j >= 0:
                        potential_name = lines[i - j].strip()
                        # Name line should not start with common labels and should not be empty
                        if (potential_name and
                            not potential_name.startswith(("Title:", "LinkedIn:", "Tenure:", "Location:", "Insight:", "CRM", "Marketing", "ABM", "Sales", "Analytics", "Event", "Other", "Conversational", "CMS"))):
                            # This is likely the name - create hyperlink
                            name_line_start = sum(len(lines[k]) + 1 for k in range(i - j))
                            name_line_end = name_line_start + len(potential_name)

                            format_requests.append({
                                "updateTextStyle": {
                                    "range": {
                                        "startIndex": name_line_start + 1,
                                        "endIndex": name_line_end + 1
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
                            break

        current_pos = line_end + 1  # +1 for newline

    return format_requests


def create_google_doc(company_name, company_research, techstack, contacts, news_and_activity=""):
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
        text_format_requests = apply_text_formatting(docs_service, doc_id, full_text)
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
    if len(sys.argv) != 3:
        print("Usage: python deal_research.py \"Company Name\" \"domain.com\"")
        print("Example: python deal_research.py \"Asana\" \"asana.com\"")
        sys.exit(1)

    company_name = sys.argv[1]
    domain = sys.argv[2]

    print(f"\n{'='*60}")
    print(f"Deal Research Generator")
    print(f"{'='*60}")
    print(f"Company: {company_name}")
    print(f"Domain: {domain}")
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

    # Step 5: Gather recent news and activity using Tavily
    print("\n[Step 5/6] Gathering recent news and activity...")
    news_and_activity = generate_news_and_activity(company_name, domain)
    if not news_and_activity:
        news_and_activity = "Error gathering news. Please add manually."

    # Step 6: Create Google Doc
    doc_url = create_google_doc(company_name, company_research, techstack, contacts, news_and_activity)

    # Summary
    print(f"\n{'='*60}")
    print("COMPLETE!")
    print(f"{'='*60}")
    if doc_url:
        print(f"\nGoogle Doc: {doc_url}")
    print(f"\nData collected:")
    print(f"  - Apollo enrichment: {'Success' if apollo_data.get('industry') else 'Partial'}")
    print(f"  - Tech stack detected: {len(scraped_tech)} technologies from website")
    print(f"  - LinkedIn contacts: Tavily + Gemini search completed")
    print(f"  - News & Activity: Tavily search completed")
    print(f"\n{'='*60}\n")


if __name__ == "__main__":
    main()
