# Google Trends Strategy for Brand Health Index

## 🎯 **Strategic Problem Solved**

**Issue**: The original Google Trends configuration had inconsistent search categories across financial brands, making brand comparison impossible and Brand Health Index calculations meaningless.

**Solution**: Standardized all brands to use identical category terms, enabling accurate competitive analysis and meaningful brand health metrics.

## 📊 **Data Collection Strategy**

### **Consistent Categories Across All Brands**
All financial institutions now track the same 6 core banking categories:

1. **`savings account`** - Basic banking, universal need
2. **`checking account`** - Basic banking, universal need  
3. **`credit card`** - Consumer credit, high search volume
4. **`mortgage rates`** - Major life decision, high consideration
5. **`personal loan`** - Consumer credit, comparison shopping
6. **`mobile banking`** - Digital experience, growing importance

### **Multi-Dimensional Search Analysis**

#### **1. Brand Awareness** (`brand` type)
- Pure brand searches: "JPMorgan Chase", "Wells Fargo", "TD Bank"
- Measures: Overall brand recognition and top-of-mind awareness

#### **2. Market Size** (`category` type)  
- Category searches: "savings account", "mortgage rates"
- Measures: Total market demand for financial products

#### **3. Brand Consideration** (`brand_category` type)
- Combined searches: "Chase Bank savings account", "Wells Fargo mortgage rates"
- Measures: Intent to consider specific brands for specific products

#### **4. Category-First Intent** (`category_brand` type)
- Reverse searches: "savings account Wells Fargo", "mortgage rates TD Bank"
- Measures: Product-first research leading to brand consideration

## 🏆 **Brand Health Index Benefits**

### **Accurate Competitive Analysis**
```
Brand Share of Voice = (Brand Category Searches) / (Total Category Searches) × 100
```

### **Intent-Based Scoring**
- **High Intent**: "TD Bank mortgage rates" (ready to apply)
- **Medium Intent**: "TD Bank" + "mortgage rates" (researching)  
- **Low Intent**: "TD Bank" (general awareness)

### **Geographic Insights**
Tracking across 5 key US markets:
- **US** (national trends)
- **US-NY** (financial hub)
- **US-CA** (tech-forward market)
- **US-TX** (growth market)
- **US-FL** (retiree market)

## 📈 **Data Structure**

```json
{
  "brand_id": "td",
  "keyword": "TD Bank savings account",
  "keyword_type": "brand_category",
  "geo": "US-NY", 
  "ts_event": "2025-09-24T00:00:00",
  "value": 75,
  "is_brand_keyword": true,
  "related_queries_top": ["td bank savings rates", "td bank online"],
  "collected_at": "2025-09-24T04:17:52"
}
```

## 🎯 **Strategic Advantages**

### **1. Apples-to-Apples Comparison**
All brands measured against identical categories eliminates data bias

### **2. Comprehensive Coverage** 
Captures the full customer journey from awareness to consideration

### **3. Intent Signals**
Brand + category combinations reveal purchase intent strength

### **4. Market Context**
Category-only searches provide market sizing and trend context

### **5. Geographic Precision**
Regional insights enable targeted marketing and expansion decisions

## 🚀 **Implementation Impact**

**Before**: Inconsistent categories made brand comparison meaningless
- JPMorgan: "best savings account" vs Wells Fargo: "home loan" ❌

**After**: Standardized categories enable precise competitive analysis  
- All brands: "savings account", "mortgage rates", "credit card" ✅

This ensures the **Brand Health Index accurately reflects true competitive positioning** rather than measurement artifacts from inconsistent data collection.

## 📋 **Next Steps**

1. **Deploy Updated Trends Fetcher** ✅ (Completed)
2. **Configure Fivetran** to ingest the enhanced trends data
3. **Update dbt Models** to leverage the new keyword_type classifications
4. **Build Dashboards** showing competitive brand health metrics
5. **Set Alerts** for significant changes in brand consideration trends

---

**Result**: TD Bank and all tracked financial institutions now have consistent, comparable Google Trends data for accurate Brand Health Index calculations. 🏦📊
