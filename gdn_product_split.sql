# DBM
-- 'DBM AdX Display', 'DBM AdX Non-YT Display', 'DBM Non-Google Display',
-- 'DBM AdX YouTube Display', 'DBM AdX Non-YT Video', 'DBM Non-YT Video',
-- 'DBM Non-Google Video', 'GDN AwBid Video', 'DBM AdX YouTube Video',
-- 'DBM YouTube Video', 'DBM AdX Video', 'DBM Reserve YouTube Video'

# GDN
-- 'GDN Apps Display', 'GDN Awareness', 'GDN Display', 'GDN GOO DART',
-- 'GDN GVN Display', 'GDN GVN Lightbox Display', 'GDN GVN Viewable Display',
-- 'GDN Lightbox Display', 'GDN Unmigrated Admob Display', 'GDN Viewable Display',
-- 'GDN AwBid Display', 'YouTube Display', 'YouTube Lightbox Display',
-- 'YouTube Viewable Display', 'GDN Apps Instream Video', 'GDN Apps Video',
-- 'GDN GVN InDisplay Video', 'GDN GVN Instream Video', 'GDN GVN Video',
-- 'GDN InDisplay Video', 'GDN Instream Video', 'GDN Video'
# Gmail
# 'Gmail Message Ads AWFE', 'Gmail Message Ads Crush',
#
# # Search
# 'AFS', 'ComputerTablet Google.com', 'ComputerTablet Google.com PLA',
# 'ComputerTablet Hotel Price Ads', 'Mobile Google.com', 'Mobile Google.com PLA',
# 'Mobile Hotel Price Ads',
#
# # YT
# 'YouTube InDisplay Video', 'YouTube Instream Video', 'YouTube Video',
# 'YouTube MastHead Video'

  SET accounting_group analytics-internal-processing-dev;
  SET min_completion_ratio 1;
  SET io_timeout 2400;
  SET nest_join_schema true;
  SET runtime_name dremel;
  SET materialize_overwrite true;
  SET materialize_owner_group analytics-internal-processing-dev;
  SET run_as_mdb_account aredakov;

  MATERIALIZE '/cns/ig-d/home/aredakov/growth_gdn/gdn_divisions/data@500' AS
  SELECT
    company_rollup.division_id AS gdn_divisions
  FROM XP_DailyCurrentStats_F
  WHERE billing_category = 'Billable'
    AND product_google_business = 'Advertising'
    AND product IN
      (
      'GDN Apps Display', 'GDN Awareness', 'GDN Display', 'GDN GOO DART',
      'GDN GVN Display', 'GDN GVN Lightbox Display', 'GDN GVN Viewable Display',
      'GDN Lightbox Display', 'GDN Unmigrated Admob Display', 'GDN Viewable Display',
      'GDN AwBid Display', 'YouTube Display', 'YouTube Lightbox Display',
      'YouTube Viewable Display', 'GDN Apps Instream Video', 'GDN Apps Video',
      'GDN GVN InDisplay Video', 'GDN GVN Instream Video', 'GDN GVN Video',
      'GDN InDisplay Video', 'GDN Instream Video', 'GDN Video'
      )
    AND date_id > INT64(FLOOR((DATE_ADD(NOW(), -30, 'DAY')
    - parse_time_usec('2000-01-01')) / 3600 / 24/ 1000000))
    AND delivered_revenue.revenue_usd_quarterly_fx > 0
  GROUP@500 BY 1,2;

library(ginstall)
library(gfile)
library(namespacefs)
library(rglib)
library(cfs)
library(dremel)
library(ggplot2)
library(scales)
library(directlabels)
library(lubridate)
library(Hmisc)
library(nlme)
library(lme4)
library(data.table)
InitGoogle()
options("scipen"=100, "digits"=4)

myConn <- DremelConnect()
DremelSetMinCompletionRatio(myConn, 1.0)
DremelSetAccountingGroup(myConn,'urchin-processing-qa')
DremelSetMaterializeOwnerGroup(myConn, 'materialize-a-dremel')
DremelSetMaterializeOverwrite(myConn, TRUE)
DremelSetIOTimeout(myConn, 7200)

# GDN product slices.
DremelAddTableDef('gdn_divisions', '/cns/ig-d/home/aredakov/growth_gdn/gdn_divisions/data*',
  myConn, verbose=FALSE)

# By product spend.
p <- DremelExecuteQuery("
  SELECT
    pro.division_id AS division_id,
    product_group,
    prod_revenue,
    total_revenue,
    prod_revenue / total_revenue AS share_revenue,
  FROM
    (SELECT
      company_rollup.division_id AS division_id,
      CASE
        WHEN product IN
          ('DBM AdX Display', 'DBM AdX Non-YT Display', 'DBM Non-Google Display',
          'DBM AdX YouTube Display', 'DBM AdX Non-YT Video', 'DBM Non-YT Video',
          'DBM Non-Google Video', 'GDN AwBid Video', 'DBM AdX YouTube Video',
          'DBM YouTube Video', 'DBM AdX Video', 'DBM Reserve YouTube Video')
        THEN 'DBM'
        WHEN product IN
          ('GDN Apps Display', 'GDN Awareness', 'GDN Display', 'GDN GOO DART',
          'GDN GVN Display', 'GDN GVN Lightbox Display', 'GDN GVN Viewable Display',
          'GDN Lightbox Display', 'GDN Unmigrated Admob Display', 'GDN Viewable Display',
          'GDN AwBid Display', 'YouTube Display', 'YouTube Lightbox Display',
          'YouTube Viewable Display', 'GDN Apps Instream Video', 'GDN Apps Video',
          'GDN GVN InDisplay Video', 'GDN GVN Instream Video', 'GDN GVN Video',
          'GDN InDisplay Video', 'GDN Instream Video', 'GDN Video')
        THEN 'GDN'
        WHEN product IN
          ('Gmail Message Ads AWFE', 'Gmail Message Ads Crush')
        THEN 'Gmail'
        WHEN product IN
         ('AFS', 'ComputerTablet Google.com', 'ComputerTablet Google.com PLA',
        'ComputerTablet Hotel Price Ads', 'Mobile Google.com',
        'Mobile Google.com PLA', 'Mobile Hotel Price Ads')
        THEN 'Search'
        WHEN product IN
         ('YouTube InDisplay Video', 'YouTube Instream Video', 'YouTube Video',
        'YouTube MastHead Video')
        THEN 'YouTube'
      END AS product_group,
      SUM(delivered_revenue.revenue_usd_quarterly_fx) AS prod_revenue
    FROM XP_DailyCurrentStats_F xp
    JOIN@50 gdn_divisions b
    ON division_id = gdn_divisions
    WHERE billing_category = 'Billable'
      AND product_google_business = 'Advertising'
      AND product IN
        (
        'DBM AdX Display', 'DBM AdX Non-YT Display', 'DBM Non-Google Display',
        'DBM AdX YouTube Display', 'DBM AdX Non-YT Video', 'DBM Non-YT Video',
        'DBM Non-Google Video', 'GDN AwBid Video', 'DBM AdX YouTube Video',
        'DBM YouTube Video', 'DBM AdX Video', 'DBM Reserve YouTube Video',
        'GDN Apps Display', 'GDN Awareness', 'GDN Display', 'GDN GOO DART',
        'GDN GVN Display', 'GDN GVN Lightbox Display', 'GDN GVN Viewable Display',
        'GDN Lightbox Display', 'GDN Unmigrated Admob Display', 'GDN Viewable Display',
        'GDN AwBid Display', 'YouTube Display', 'YouTube Lightbox Display',
        'YouTube Viewable Display', 'GDN Apps Instream Video', 'GDN Apps Video',
        'GDN GVN InDisplay Video', 'GDN GVN Instream Video', 'GDN GVN Video',
        'GDN InDisplay Video', 'GDN Instream Video', 'GDN Video',
        'Gmail Message Ads AWFE', 'Gmail Message Ads Crush',
        'AFS', 'ComputerTablet Google.com', 'ComputerTablet Google.com PLA',
        'ComputerTablet Hotel Price Ads', 'Mobile Google.com',
        'Mobile Google.com PLA', 'Mobile Hotel Price Ads',
        'YouTube InDisplay Video', 'YouTube Instream Video', 'YouTube Video',
        'YouTube MastHead Video'
        )
      AND date_id > INT64(FLOOR((DATE_ADD(NOW(), -31, 'DAY')
      - parse_time_usec('2000-01-01')) / 3600 / 24/ 1000000))
    GROUP@50 BY 1,2) pro
  JOIN@50
    (SELECT
      company_rollup.division_id AS division_id,
      SUM(delivered_revenue.revenue_usd_quarterly_fx) AS total_revenue
    FROM XP_DailyCurrentStats_F
    JOIN@50 gdn_divisions b
    ON division_id = gdn_divisions
    WHERE billing_category = 'Billable'
      AND product_google_business = 'Advertising'
      AND product IN
        (
        'DBM AdX Display', 'DBM AdX Non-YT Display', 'DBM Non-Google Display',
        'DBM AdX YouTube Display', 'DBM AdX Non-YT Video', 'DBM Non-YT Video',
        'DBM Non-Google Video', 'GDN AwBid Video', 'DBM AdX YouTube Video',
        'DBM YouTube Video', 'DBM AdX Video', 'DBM Reserve YouTube Video',
        'GDN Apps Display', 'GDN Awareness', 'GDN Display', 'GDN GOO DART',
        'GDN GVN Display', 'GDN GVN Lightbox Display', 'GDN GVN Viewable Display',
        'GDN Lightbox Display', 'GDN Unmigrated Admob Display', 'GDN Viewable Display',
        'GDN AwBid Display', 'YouTube Display', 'YouTube Lightbox Display',
        'YouTube Viewable Display', 'GDN Apps Instream Video', 'GDN Apps Video',
        'GDN GVN InDisplay Video', 'GDN GVN Instream Video', 'GDN GVN Video',
        'GDN InDisplay Video', 'GDN Instream Video', 'GDN Video',
        'Gmail Message Ads AWFE', 'Gmail Message Ads Crush',
        'AFS', 'ComputerTablet Google.com', 'ComputerTablet Google.com PLA',
        'ComputerTablet Hotel Price Ads', 'Mobile Google.com',
        'Mobile Google.com PLA', 'Mobile Hotel Price Ads',
        'YouTube InDisplay Video', 'YouTube Instream Video', 'YouTube Video',
        'YouTube MastHead Video'
        )
      AND date_id > INT64(FLOOR((DATE_ADD(NOW(), -31, 'DAY')
      - parse_time_usec('2000-01-01')) / 3600 / 24/ 1000000))
    GROUP@50 BY 1) tot
  ON pro.division_id = tot.division_id
;", myConn)

rhg_cols <- c("#771C19","#AA3929","#E25033","#F27314","#F8A31B","#E2C59F",
  "#B6C5CC","#8E9CA3","#556670","#000000")

# Below 25th percentile.
pp <- p[ which(p$total_revenue <= quantile(p$total_revenue, c(.25))), ]
pp <- na.omit(pp)
medians <- ddply(pp, .(product_group), summarise, med = median(share_revenue))
pp$product_group <- factor(pp$product_group, levels= c("Search","GDN",
  "YouTube","Gmail","DBM"))

dodge <- position_dodge(width = 0.4)
ggplot(pp, aes(x=product_group, y=share_revenue, fill=product_group)) +
geom_boxplot(outlier.shape = NA) +
scale_y_continuous(labels =  percent) +
coord_cartesian(ylim = c(0, 1.1)) +
coord_flip()  +
scale_fill_manual(values = rhg_cols) +
geom_text(data = medians, aes(x = product_group, y = med,
  label=sprintf("%1.2f%%", med*100)), size = 4, vjust = -2) +
theme(legend.position="none") +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Products' Share of Total Revenue. Below 25th Percentile.") +
ylab("Revenue Share Distiribution") + xlab("Product")

pp_c <- ddply(pp, .(product_group), summarise, cnt = length(share_revenue))
pp_c$product_group <- factor(pp_c$product_group, levels= c("Search","GDN",
  "YouTube","Gmail","DBM"))
ggplot(pp_c, aes(x=product_group, y=cnt ,fill=product_group)) +
geom_bar(stat="identity") +
geom_text(aes(label=sprintf("%1.1f", cnt)),vjust=-0.1,
  size=4) +
theme(legend.position="none") +
coord_flip()  +
scale_fill_manual(values = rhg_cols) +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Count of Divisions per Products. Below 25th Percentile.") +
ylab("Count of Divisions") + xlab("Product")

# Between 25th percentile and 50th.
pf <- p[ which(p$total_revenue > quantile(p$total_revenue, c(.25)) &
  p$total_revenue <= quantile(p$total_revenue, c(.5))), ]
pf <- na.omit(pf)
medians <- ddply(pf, .(product_group), summarise, med = median(share_revenue))
pf$product_group <- factor(pf$product_group, levels= c("Search","GDN",
  "YouTube","Gmail","DBM"))

dodge <- position_dodge(width = 0.4)
ggplot(pf, aes(x=product_group, y=share_revenue, fill=product_group)) +
geom_boxplot(outlier.shape = NA) +
scale_y_continuous(labels =  percent) +
coord_cartesian(ylim = c(0, 1.1)) +
coord_flip()  +
scale_fill_manual(values = rhg_cols) +
geom_text(data = medians, aes(x = product_group, y = med,
  label=sprintf("%1.2f%%", med*100)), size = 4, vjust = -2) +
theme(legend.position="none") +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Products' Share of Total Revenue. Above 25th & Below 50th.") +
ylab("Revenue Share Distiribution") + xlab("Product")

pf_c <- ddply(pf, .(product_group), summarise, cnt = length(share_revenue))
pf_c$product_group <- factor(pf_c$product_group, levels= c("Search","GDN",
  "YouTube","Gmail","DBM"))
ggplot(pf_c, aes(x=product_group, y=cnt ,fill=product_group)) +
geom_bar(stat="identity") +
geom_text(aes(label=sprintf("%1.1f", cnt)),vjust=-0.1,
  size=4) +
theme(legend.position="none") +
coord_flip()  +
scale_fill_manual(values = rhg_cols) +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Count of Divisions per Products. Above 25th & Below 50th.") +
ylab("Count of Divisions") + xlab("Product")

# Between 50th and 75th.
ps <- p[ which(p$total_revenue > quantile(p$total_revenue, c(.50)) &
  p$total_revenue <= quantile(p$total_revenue, c(.75))), ]
ps <- na.omit(ps)
medians <- ddply(ps, .(product_group), summarise, med = median(share_revenue))
ps$product_group <- factor(ps$product_group, levels= c("Search","GDN",
  "YouTube","Gmail","DBM"))

dodge <- position_dodge(width = 0.4)
ggplot(ps, aes(x=product_group, y=share_revenue, fill=product_group)) +
geom_boxplot(outlier.shape = NA) +
scale_y_continuous(labels =  percent) +
coord_cartesian(ylim = c(0, 1.1)) +
coord_flip()  +
scale_fill_manual(values = rhg_cols) +
geom_text(data = medians, aes(x = product_group, y = med,
  label=sprintf("%1.2f%%", med*100)), size = 4, vjust = -2) +
theme(legend.position="none") +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Products' Share of Total Revenue. Above 50th & Below 75th.") +
ylab("Revenue Share Distiribution") + xlab("Product")

ps_c <- ddply(ps, .(product_group), summarise, cnt = length(share_revenue))
ps_c$product_group <- factor(ps_c$product_group, levels= c("Search","GDN",
  "YouTube","Gmail","DBM"))
ggplot(ps_c, aes(x=product_group, y=cnt ,fill=product_group)) +
geom_bar(stat="identity") +
geom_text(aes(label=sprintf("%1.1f", cnt)),vjust=-0.1,
  size=4) +
theme(legend.position="none") +
coord_flip()  +
scale_fill_manual(values = rhg_cols) +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Count of Divisions per Products. Above 50th & Below 75th.") +
ylab("Count of Divisions") + xlab("Product")

# Between 75th and 90th.
pn <- p[ which(p$total_revenue > quantile(p$total_revenue, c(.75)) &
  p$total_revenue <= quantile(p$total_revenue, c(.90))), ]
pn <- na.omit(pn)
medians <- ddply(pn, .(product_group), summarise, med = median(share_revenue))
pn$product_group <- factor(pn$product_group, levels= c("Search","GDN",
  "YouTube","Gmail","DBM"))

dodge <- position_dodge(width = 0.4)
ggplot(pn, aes(x=product_group, y=share_revenue, fill=product_group)) +
geom_boxplot(outlier.shape = NA) +
scale_y_continuous(labels =  percent) +
coord_cartesian(ylim = c(0, 1.1)) +
coord_flip()  +
scale_fill_manual(values = rhg_cols) +
geom_text(data = medians, aes(x = product_group, y = med,
  label=sprintf("%1.2f%%", med*100)), size = 4, vjust = -2) +
theme(legend.position="none") +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Products' Share of Total Revenue. Above 75th & Below 90th.") +
ylab("Revenue Share Distiribution") + xlab("Product")

pn_c <- ddply(pn, .(product_group), summarise, cnt = length(share_revenue))
pn_c$product_group <- factor(pn_c$product_group, levels= c("Search","GDN",
  "YouTube","Gmail","DBM"))
ggplot(pn_c, aes(x=product_group, y=cnt ,fill=product_group)) +
geom_bar(stat="identity") +
geom_text(aes(label=sprintf("%1.1f", cnt)),vjust=-0.1,
  size=4) +
theme(legend.position="none") +
coord_flip()  +
scale_fill_manual(values = rhg_cols) +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Count of Divisions per Products. Above 75th & Below 90th.") +
ylab("Count of Divisions") + xlab("Product")

# Between 90th and 95th.
pnn <- p[ which(p$total_revenue > quantile(p$total_revenue, c(.90)) &
  p$total_revenue <= quantile(p$total_revenue, c(.95))), ]
pnn <- na.omit(pnn)
medians <- ddply(pnn, .(product_group), summarise, med = median(share_revenue))
pnn$product_group <- factor(pnn$product_group, levels= c("Search","GDN",
  "YouTube","Gmail","DBM"))

dodge <- position_dodge(width = 0.4)
ggplot(pnn, aes(x=product_group, y=share_revenue, fill=product_group)) +
geom_boxplot(outlier.shape = NA) +
scale_y_continuous(labels =  percent) +
coord_cartesian(ylim = c(0, 1.1)) +
coord_flip()  +
scale_fill_manual(values = rhg_cols) +
geom_text(data = medians, aes(x = product_group, y = med,
  label=sprintf("%1.2f%%", med*100)), size = 4, vjust = -2) +
theme(legend.position="none") +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Products' Share of Total Revenue. Above 90th & Below 95th.") +
ylab("Revenue Share Distiribution") + xlab("Product")

pnn_c <- ddply(pnn, .(product_group), summarise, cnt = length(share_revenue))
pnn_c$product_group <- factor(pnn_c$product_group, levels= c("Search","GDN",
  "YouTube","Gmail","DBM"))
ggplot(pnn_c, aes(x=product_group, y=cnt ,fill=product_group)) +
geom_bar(stat="identity") +
geom_text(aes(label=sprintf("%1.1f", cnt)),vjust=-0.1,
  size=4) +
theme(legend.position="none") +
coord_flip()  +
scale_fill_manual(values = rhg_cols) +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Count of Divisions per Products. Above 90th & Below 95th.") +
ylab("Count of Divisions") + xlab("Product")

# Above 99th.
pt <- p[ which(p$total_revenue >= quantile(p$total_revenue, c(.99))), ]
pt <- na.omit(pt)
medians <- ddply(pt, .(product_group), summarise, med = median(share_revenue))
pt$product_group <- factor(pt$product_group, levels= c("Search","GDN",
  "YouTube","Gmail","DBM"))

dodge <- position_dodge(width = 0.4)
ggplot(pt, aes(x=product_group, y=share_revenue, fill=product_group)) +
geom_boxplot(outlier.shape = NA) +
scale_y_continuous(labels =  percent) +
coord_cartesian(ylim = c(0, 1.1)) +
coord_flip()  +
scale_fill_manual(values = rhg_cols) +
geom_text(data = medians, aes(x = product_group, y = med,
  label=sprintf("%1.2f%%", med*100)), size = 4, vjust = -2) +
theme(legend.position="none") +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Products' Share of Total Revenue. Above 99th.") +
ylab("Revenue Share Distiribution") + xlab("Product")

pt_c <- ddply(pt, .(product_group), summarise, cnt = length(share_revenue))
pt_c$product_group <- factor(pt_c$product_group, levels= c("Search","GDN",
  "YouTube","Gmail","DBM"))
ggplot(pt_c, aes(x=product_group, y=cnt ,fill=product_group)) +
geom_bar(stat="identity") +
geom_text(aes(label=sprintf("%1.1f", cnt)),vjust=-0.1,
  size=4) +
theme(legend.position="none") +
coord_flip()  +
scale_fill_manual(values = rhg_cols) +
theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
ggtitle("Count of Divisions per Products. Above 99th.") +
ylab("Count of Divisions") + xlab("Product")

