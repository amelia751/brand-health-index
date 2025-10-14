#!/usr/bin/env python3
"""
Test TD Bank Keyword Coverage
Verify that our enhanced TD Bank keywords capture all customer references
"""

# ULTRA COMPREHENSIVE TD Bank keywords from our updated fetcher
TD_BANK_KEYWORDS = [
    # === OFFICIAL CORPORATE NAMES ===
    'TD Bank', 'TD Bank N.A.', 'TD Bank USA', 'TD Bank US', 'TD Bank National Association',
    'Toronto Dominion Bank', 'Toronto-Dominion Bank', 'Toronto Dominion', 'Toronto-Dominion',
    'The Toronto-Dominion Bank', 'TD Bank Group',
    
    # === ABBREVIATIONS & VARIATIONS ===
    'TD', 'TDB', 'TD-Bank', 'T-D', 'T.D.', 'T D Bank', 'T.D. Bank',
    'TD Banknorth', 'TD Waterhouse', 'TD Securities',
    
    # === MAJOR SUBSIDIARIES & DIVISIONS ===
    'TD Canada Trust', 'TD Ameritrade', 'TD Auto Finance', 'TD Wealth Management',
    'TD Direct Investing', 'TD Insurance', 'TD Commercial Banking', 'TD Private Banking',
    'TD Asset Management', 'TD Securities', 'TD Investment Services',
    
    # === SPECIFIC CREDIT CARD PRODUCTS ===
    'TD Cash Credit Card', 'TD Cash Back Credit Card', 'TD Double Up Credit Card',
    'TD First Class Travel Visa', 'TD Aeroplan Visa', 'TD Business Cash Back Visa',
    'TD Business Travel Visa', 'TD Student Credit Card', 'TD Secured Credit Card',
    'TD Rewards Credit Card', 'TD Platinum Travel Visa', 'TD Gold Elite Visa',
    
    # === CHECKING ACCOUNT PRODUCTS ===
    'TD Convenience Checking', 'TD Beyond Checking', 'TD Premier Checking',
    'TD Student Checking', 'TD 60+ Checking', 'TD Business Checking',
    'TD Simple Business Checking', 'TD Commercial Checking',
    
    # === SAVINGS & INVESTMENT PRODUCTS ===
    'TD Simple Savings', 'TD Growth Money Market', 'TD Special Rate CD',
    'TD Choice Promotional CD', 'TD Signature Select', 'TD IRA',
    'TD Traditional IRA', 'TD Roth IRA', 'TD SEP IRA', 'TD Simple IRA',
    
    # === MORTGAGE & LOAN PRODUCTS ===
    'TD Mortgage', 'TD Home Equity', 'TD Personal Loan', 'TD Student Loan',
    'TD Auto Loan', 'TD Business Loan', 'TD Line of Credit', 'TD HELOC',
    'TD Fixed Rate Mortgage', 'TD Adjustable Rate Mortgage', 'TD Jumbo Mortgage',
    
    # === DIGITAL SERVICES ===
    'TD EasyWeb', 'TD Mobile App', 'TD Online Banking', 'TD Digital Banking',
    'TD Mobile Deposit', 'TD Zelle', 'TD Bill Pay', 'TD MySpend',
    'TD GoalAssist', 'TD Clari', 'TD Mobile Banking',
    
    # === CUSTOMER SERVICE & LOCATIONS ===
    'TD Customer Service', 'TD Branch', 'TD ATM', 'TD Teller', 'TD Call Center',
    'TD Customer Care', 'TD Support', 'TD Help Desk', 'TD Phone Banking',
    
    # === CONTEXTUAL PATTERNS (case variations) ===
    'TD checking', 'TD savings', 'TD credit', 'TD debit', 'TD card',
    'TD account', 'TD online', 'TD app', 'TD mortgage', 'TD loan',
    'td bank', 'td', 'td checking', 'td savings', 'td credit card',
    'td online banking', 'td mobile app', 'td customer service',
    
    # === COMMON MISSPELLINGS & SLANG ===
    'TD Bankz', 'T D Bank', 'TD Can Trust', 'TD Ameritrd', 'TD Amertrade',
    'Toronto Dom', 'Toronto-Dom', 'TD Banc', 'TDBank', 'TD_Bank',
    
    # === REGIONAL & COLLOQUIAL REFERENCES ===
    'TD Green', 'TD Canada', 'TD US', 'TD USA', 'TD American',
    'TD North', 'TD East Coast', 'TD New England', 'TD Northeast',
    
    # === BUSINESS & COMMERCIAL SERVICES ===
    'TD Business Banking', 'TD Commercial', 'TD Corporate Banking',
    'TD Treasury Services', 'TD Merchant Services', 'TD Business Credit Card',
    'TD Commercial Real Estate', 'TD Equipment Finance',
    
    # === INVESTMENT & WEALTH SERVICES ===
    'TD Wealth', 'TD Investment', 'TD Portfolio', 'TD Financial Planning',
    'TD Private Investment Counsel', 'TD Asset Management USA',
    'TD Epoch', 'TD Greystone', 'TDAM',
    
    # === HISTORICAL & LEGACY NAMES ===
    'Commerce Bank', 'TD Commerce', 'Banknorth', 'TD Banknorth Group'
]

def test_keyword_matching(text_samples):
    """Test how many TD Bank references our keywords catch"""
    
    matched = 0
    total = len(text_samples)
    
    print("🧪 Testing TD Bank Keyword Coverage...")
    print(f"📝 Testing {total} sample texts\n")
    
    for i, sample in enumerate(text_samples, 1):
        found_keywords = []
        
        # Check each keyword
        for keyword in TD_BANK_KEYWORDS:
            if keyword.lower() in sample.lower():
                found_keywords.append(keyword)
        
        if found_keywords:
            matched += 1
            status = "✅"
            keywords_str = ", ".join(found_keywords[:3])  # Show first 3 matches
            if len(found_keywords) > 3:
                keywords_str += f" (+{len(found_keywords)-3} more)"
        else:
            status = "❌"
            keywords_str = "No matches"
        
        print(f"{status} Sample {i}: {keywords_str}")
        print(f"   Text: {sample[:80]}...")
        print()
    
    coverage = (matched / total) * 100
    print(f"📊 Coverage Results:")
    print(f"   Matched: {matched}/{total} samples")
    print(f"   Coverage: {coverage:.1f}%")
    
    if coverage >= 90:
        print("🎉 Excellent keyword coverage!")
    elif coverage >= 75:
        print("✅ Good keyword coverage")
    else:
        print("⚠️ Keyword coverage needs improvement")
    
    return coverage

def suggest_missing_keywords(text_samples):
    """Analyze unmatched samples to suggest additional keywords"""
    
    print("\n🔍 Analyzing potential missing keywords...")
    
    # Common TD Bank reference patterns we might be missing
    potential_keywords = [
        'TD Financial', 'TD Group', 'TD Securities', 'TD Wealth',
        'TD Insurance', 'TD Investment', 'TD Private Banking',
        'TD Commercial', 'TD Business', 'TD Corporate',
        'TD Mobile', 'TD Web', 'TD Digital', 'TD Electronic',
        'TD Direct', 'TD Express', 'TD Quick', 'TD Easy',
        'TD Premier', 'TD Select', 'TD Plus', 'TD Gold',
        'TD Student', 'TD Senior', 'TD Youth', 'TD Kids'
    ]
    
    found_suggestions = []
    
    for sample in text_samples:
        # Check if sample has TD context but wasn't matched
        if 'td' in sample.lower() and not any(kw.lower() in sample.lower() for kw in TD_BANK_KEYWORDS):
            # Check for potential missing keywords
            for potential in potential_keywords:
                if potential.lower() in sample.lower():
                    found_suggestions.append(potential)
    
    if found_suggestions:
        unique_suggestions = list(set(found_suggestions))
        print(f"💡 Suggested additional keywords:")
        for suggestion in unique_suggestions[:10]:  # Show top 10
            print(f"   - {suggestion}")
    else:
        print("✅ No obvious missing keywords found")

# COMPREHENSIVE TD Bank customer texts (realistic examples covering all product categories)
SAMPLE_TEXTS = [
    # Basic banking references
    "I've been with TD Bank for 5 years and love their customer service",
    "TD has the worst fees I've ever seen at a bank",
    "My TD checking account was charged an overdraft fee again",
    "Toronto Dominion Bank just approved my mortgage application",
    "The TD app keeps crashing when I try to deposit checks",
    
    # Specific product mentions
    "TD Ameritrade has great investment options for beginners", 
    "I just got approved for the TD Cash Credit Card with 2% back",
    "My TD Double Up Credit Card rewards are amazing for groceries",
    "TD First Class Travel Visa gave me free checked bags",
    "TD Convenience Checking has no minimum balance requirement",
    "TD Beyond Checking comes with a free safety deposit box",
    "TD Premier Checking waived all my fees this month",
    "TD Student Checking is perfect for college kids",
    "TD Simple Savings has terrible interest rates",
    "TD Growth Money Market requires $2500 minimum",
    "TD Special Rate CD is offering 4.5% for 12 months",
    
    # Digital services
    "TD EasyWeb online banking is so user-friendly",
    "TD Mobile App lets me deposit checks instantly",
    "TD Zelle transfers are super fast between friends",
    "TD Bill Pay saved me so much time this month",
    "TD MySpend helps me track my spending habits",
    "TD GoalAssist is helping me save for vacation",
    
    # Loan and mortgage products
    "TD Auto Finance gave me a terrible interest rate on my car loan",
    "TD Home Equity loan helped me renovate my kitchen",
    "TD Personal Loan approval was surprisingly quick",
    "TD Student Loan rates are competitive with federal loans",
    "TD HELOC gives me flexible access to home equity",
    "TD Fixed Rate Mortgage locked in at 6.5%",
    "TD Adjustable Rate Mortgage started at 5.8%",
    
    # Business banking
    "TD Business Checking has reasonable monthly fees",
    "TD Commercial Banking helped with our expansion loan",
    "TD Merchant Services processes our credit card payments",
    "TD Business Credit Card has great cash back rewards",
    
    # Investment and wealth services
    "TD Wealth Management assigned me a personal advisor",
    "TD Direct Investing platform is easy to use",
    "TD Asset Management handles my retirement portfolio",
    "TD Private Banking offers exclusive services",
    "TDAM mutual funds have low expense ratios",
    
    # Customer service and locations
    "I called TD customer service and waited 45 minutes on hold",
    "TD Bank's ATM network is pretty convenient in my area",
    "The TD branch near me has really friendly tellers",
    "TD Call Center representatives are always helpful",
    "TD Customer Care resolved my fraud issue quickly",
    
    # Regional and legacy references
    "TD Banknorth used to be my local bank before the merger",
    "Commerce Bank became TD Bank in our area",
    "TD Canada Trust is what they call it up north",
    "TD Waterhouse was my old investment platform",
    
    # Casual and misspelled references
    "td bank", "TD Bankz", "T-D Bank", "Toronto-Dom",
    "TD Can Trust", "TDBank", "TD_Bank", "T.D. Bank",
    
    # Negative experiences
    "My TD debit card was declined at the grocery store today",
    "TD online banking is down again, this is so frustrating",
    "I'm thinking of switching from TD to a credit union",
    "The TD website is so slow and outdated",
    "I hate how TD makes you wait for check deposits to clear",
    "TD charged me $35 for a $2 overdraft, ridiculous!",
    
    # Positive experiences
    "I love TD's mobile app for quick transfers",
    "TD mortgage rates are competitive compared to other banks",
    "I've had my TD account since college, never had major issues",
    "TD's cross-border banking is useful for my Canada trips",
    "Toronto-Dominion has been around forever, they're reliable",
    "TD teller helped me set up my new business account today"
]

if __name__ == "__main__":
    print("🏦 TD Bank Keyword Coverage Analysis\n")
    
    # Test current keyword coverage
    coverage = test_keyword_matching(SAMPLE_TEXTS)
    
    # Suggest improvements
    suggest_missing_keywords(SAMPLE_TEXTS)
    
    print(f"\n📋 Current TD Bank Keywords ({len(TD_BANK_KEYWORDS)} total):")
    for i, keyword in enumerate(TD_BANK_KEYWORDS, 1):
        print(f"   {i:2d}. {keyword}")
    
    print(f"\n🎯 Recommendation:")
    if coverage >= 90:
        print("   Current keywords provide excellent coverage for TD Bank references!")
    else:
        print("   Consider adding suggested keywords to improve coverage.")
    
    print(f"\n✅ Analysis complete!")
