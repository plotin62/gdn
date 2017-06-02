#!/bin/bash
# (micros_to_usd * (max_cpm + max_cpc * predicted_ctr * 1000))
# is the ECPM value that was used to rank this advertiser in the auction.

#  JustFly (cid= 134441320)

SET accounting_group analytics-internal-processing-dev;
SET min_completion_ratio 1;
SET io_timeout 2400;
SET runtime_name dremel;
SET materialize_overwrite true;
SET materialize_owner_group analytics-internal-processing-dev;

# field = adx_query_state_fields.rejected_ad_set.last_membership_timestamp_usec.
# will only be logged when our ads are lost in final auction.
# https://cs.corp.google.com/piper///depot/google3/ads/events/tmplogs/qsem.proto?q=rejected_ad_set&sq=package:piper+file://depot/google3+-file:google3/experimental&l=585

DEFINE MACRO MIN_INT64 INT64(INT64(1)<<63);
# (1 << 63) - 1
DEFINE MACRO MAX_INT64 9223372036854775807;

# Lost bids.
MATERIALIZE '/cns/ig-d/home/aredakov/bids_recency/lost_bids_stats/data' AS
SELECT
  customer_id CustomerId,
  TypeRMKT,
  CASE
    WHEN Platform = 2 THEN 'desktop'
    WHEN Platform = 3 THEN 'mobile'
    WHEN Platform = 4 THEN 'tablet'
  END Platform,
  ROUND(NTH(51,QUANTILES(DeltaMin,101)),3) MedianDeltaMin,
  NTH(51,QUANTILES(WinnerEcpm,101)) MedianWinnerEcpm,
  NTH(51,QUANTILES(OurEcpm,101)) MedianOurEcpm,
  ROUND(AVG(DeltaMin),3) MeanDeltaMin,
  ROUND(AVG(WinnerEcpm),3) MeanWinnerEcpm,
  ROUND(AVG(OurEcpm),3) MeanOurEcpm
FROM
  (SELECT
    adx_query_state_fields.rejected_ad_set.adgroup_id AdgroupId,
    (IF(adx_query_state_fields.winner_cpm_bid_usd_micros = $MIN_INT64,
      $MAX_INT64, adx_query_state_fields.winner_cpm_bid_usd_micros)) / 1000000
      WinnerEcpm,
    (adx_query_state_fields.rejected_ad_set.cpm_bid_usd_micros / 1000000) OurEcpm,
    ((query_id.time_usec -
        adx_query_state_fields.rejected_ad_set.last_membership_timestamp_usec)
          / 1000000 / 60) DeltaMin
  FROM FLATTEN(ads.tmp_AdQueryState.last3days,
    adx_query_state_fields.rejected_ad_set)
  WHERE
    # ad_source = 2 == AdWords ads.
    adx_query_state_fields.rejected_ad_set.ad_source = 2
    # buyer_network_id = 1 == 'GDN'
    AND adx_query_state_fields.rejected_ad_set.buyer_network_id = 1
    # if last_membership_timestamp_usec > 0 then our bid LOST.
    AND adx_query_state_fields.rejected_ad_set.last_membership_timestamp_usec > 0
    AND (query_id.time_usec -
      adx_query_state_fields.rejected_ad_set.last_membership_timestamp_usec) > 0
    # We see some eCPM are negative, so this condition.
    AND adx_query_state_fields.winner_cpm_bid_usd_micros > 0
    AND country IN ('US')
    # Filtering our zero WinnerEcpm.
    HAVING WinnerEcpm > 0) a
JOIN@50
  # constant.proto
  (SELECT
    customer_id,
    adgroup_id,
    platform_type Platform,
    IF(is_dynamic = True, 'dynamic','static') TypeRMKT,
  FROM ads_programmable_stats.gdn.static_rmkt_stats
  WHERE platform_type IN (2,3,4)
    AND customer_id = 134441320
  GROUP@50 BY 1,2,3,4) b
ON AdgroupId = adgroup_id
GROUP@50 BY 1,2,3;

# Won bids.
# ad-request.proto
MATERIALIZE '/cns/ig-d/home/aredakov/bids_recency/won_bids/data' AS
SELECT
  CustomerId,
  TypeRMKT,
  Platform,
  NTH(51,QUANTILES(DeltaMin,101)) AS MedianDeltaMin,
  NTH(51,QUANTILES(GDNWonEcpm,101)) AS MedianGDNWonEcpm,
  AVG(DeltaMin) MeanDeltaMin,
  AVG(GDNWonEcpm) MeanGDNWonEcpm
FROM
(SELECT
  impression_set.customer_id CustomerId,
  impression_set.adgroup_id,
  IF(impression_set.content_impression.gpa_ad_info.use_case_name != '',
    'dynamic','static') TypeRMKT,
  CASE
    WHEN mobile_browser_class IN (0,1,2) THEN 'mobile'
    WHEN mobile_browser_class = 4 THEN 'tablet'
    WHEN mobile_browser_class = 3 THEN 'desktop'
  END Platform,
  ((query_id.time_usec  -
    impression_set.content_impression.iba_calibration_data.last_membership_timestamp_usec)
    / 1000000 / 60) AS DeltaMin,
  (impression_set.micros_to_usd *
  (impression_set.max_cpm + impression_set.predicted_ctr
    * impression_set.content_impression.auction_max_cpc * 1000)) GDNWonEcpm
FROM ads.AdQueries.last3days
WHERE impression_set.content_impression.iba_calibration_data.last_membership_timestamp_usec > 0
  # check whether the data source is remarketing:
  AND impression_set.content_impression.iba_calibration_data.user_list_source IN
    (1, 30, 105, 106, 107, 193, 235, 265, 307)
  AND country IN ('US')
  AND mobile_browser_class IN (0,1,2,3,4)
  AND STRFTIME_USEC(
    impression_set.content_impression.iba_calibration_data.last_membership_timestamp_usec,
    '%Y%m%d') >= '20170101'
  AND impression_set.customer_id = 134441320)
GROUP@50 BY 1,2,3;

# R
library(ginstall)
library(gfile)
library(namespacefs)
library(rglib)
library(cfs)
library(dremel)
library(Hmisc)
library(ggplot2)
library(scales)
library(directlabels)
library(lubridate)
library(boot)
library(gmp)
library(MASS)
library(methods)
InitGoogle()
options("scipen"=100, "digits"=12)

myConn <- DremelConnect()
DremelSetMinCompletionRatio(myConn, 1.0)
DremelSetAccountingGroup(myConn,'urchin-processing-qa')
DremelSetMaterializeOwnerGroup(myConn, 'materialize-a-dremel')
DremelSetMaterializeOverwrite(myConn, TRUE)
DremelSetIOTimeout(myConn, 7200)

#################
# WON Bids
DremelAddTableDef('WonBids', '/cns/ig-d/home/aredakov/bids_recency/won_bids/data*',
  myConn, verbose=FALSE)

w <- DremelExecuteQuery("
  SELECT
    TypeRMKT,
    Platform,
    MedianDeltaMin,
    MedianGDNWonEcpm,
    MeanDeltaMin,
    MeanGDNWonEcpm
  FROM WonBids
  WHERE MedianDeltaMin < 100000
;", myConn)

ggplot(w, aes(x = MedianDeltaMin, fill = TypeRMKT)) +
geom_density(alpha = 0.5) +
facet_grid(Platform ~ TypeRMKT) +
theme(strip.text=element_text(size=11,face="bold")) +
theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
theme(legend.position="none") +
theme(legend.title = element_blank()) +
ggtitle("Won Bids Recency") +
xlab("Min")

ww <- w[ which(w$MedianDeltaMin <= 25000), ]
ww$bin <- cut2(ww$MedianDeltaMin, g=20)
ggplot(ww, aes(x = bin, y = MedianGDNWonEcpm, fill = TypeRMKT)) +
geom_boxplot(outlier.shape = NA) +
coord_cartesian(ylim = c(0, 7.5)) +
facet_grid(Platform ~ TypeRMKT) +
theme_bw() +
scale_y_continuous(labels =  scales::dollar) +
theme(legend.position="none") +
theme(strip.text=element_text(size=11,face="bold")) +
theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
theme(axis.text.x = element_text(size=8, angle = 45, hjust = 1)) +
ggtitle("Won Bids eCPM") +
ylab("Median GDN Won Ecpm") + xlab("Bins in Minutes")

#################
# LOST Bids
DremelAddTableDef('LostBids', '/cns/ig-d/home/aredakov/bids_recency/lost_bids_stats/data*',
  myConn, verbose=FALSE)

l <- DremelExecuteQuery("
  SELECT
    TypeRMKT,
    Platform,
    MedianDeltaMin,
    MedianWinnerEcpm,
    MedianOurEcpm,
    MeanDeltaMin,
    MeanWinnerEcpm,
    MeanOurEcpm
  FROM LostBids
  WHERE MedianDeltaMin < 100000
;", myConn)

ggplot(l, aes(x = MedianDeltaMin, fill = TypeRMKT)) +
geom_density(alpha = 0.5) +
facet_grid(Platform ~ TypeRMKT) +
theme(strip.text=element_text(size=11,face="bold")) +
theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
theme(legend.position="none") +
theme(legend.title = element_blank()) +
ggtitle("Lost to Third Parties Bids Recency") +
xlab("Min")

ll <- l[ which(l$MedianDeltaMin <= 25000), ]
ll$bin <- cut2(ll$MedianDeltaMin, g=20)
ggplot(ll, aes(x = bin, y = MedianWinnerEcpm, fill = TypeRMKT)) +
geom_boxplot(outlier.shape = NA) +
coord_cartesian(ylim = c(0, 7.5)) +
facet_grid(Platform ~ TypeRMKT) +
theme_bw() +
scale_y_continuous(labels =  scales::dollar) +
theme(legend.position="none") +
theme(strip.text=element_text(size=11,face="bold")) +
theme(axis.text.x=element_text(size=11,face="bold", color="gray26")) +
theme(axis.text.y=element_text(size=11,face="bold", color="gray26")) +
theme(axis.text.x = element_text(size=8, angle = 45, hjust = 1)) +
ggtitle("Lost Bids eCPM") +
ylab("Median GDN Lost Ecpm") + xlab("Bins in Minutes")


######################
# Regression.
pd <- position_dodge(.1)
ggplot(data = d, aes(x = median_delta, y = median_eCPM)) +
geom_point(aes(x = median_delta, color="red")) +
# coord_cartesian(ylim = c(0, 1000),xlim = c(0, 600)) +
stat_smooth(position=pd, method="glm", method.args = list(family = "poisson")) +
theme_bw() +
theme(legend.position="none") +
theme(strip.text=element_text(size=14,face="bold")) +
theme(axis.text.x=element_text(size=14,face="bold", color="gray26")) +
theme(axis.text.y=element_text(size=14,face="bold", color="gray26")) +
theme(axis.text.x=element_text(size=14, color="gray26", angle = 45, hjust = 1)) +
ggtitle("Conversions Over Cookie Age. 1% Sample.") +
theme(plot.title = element_text(lineheight=.8)) +
ylab("Conversions per Cookie Age") +
xlab("Cookie Age in Days")

ld <- d[ which(d$median_delta <= 600), ]
ld$bin <- cut2(ld$median_delta, c(120,180,240,300,360,420,480,540,600))
ggplot(data = ld, aes(x = bin, y = median_eCPM)) +
geom_boxplot(width=.3, outlier.shape = NA) +
# coord_cartesian(ylim = c(0, 1000),xlim = c(0, 600)) +
  theme_bw() +
theme(legend.position="none") +
theme(strip.text=element_text(size=14,face="bold")) +
theme(axis.text.x=element_text(size=14,face="bold", color="gray26")) +
theme(axis.text.y=element_text(size=14,face="bold", color="gray26")) +
theme(axis.text.x=element_text(size=14, color="gray26", angle = 45, hjust = 1)) +
ggtitle("") +
theme(plot.title = element_text(lineheight=.8)) +
ylab("") +
xlab("")

# t-test.
fh <- d[ which(d$median_delta <= 60 ), ]
sh <- d[ which(d$median_delta > 60 & d$median_delta <= 120), ]
th <- d[ which(d$median_delta > 120 & d$median_delta <= 180), ]

t.test(fh$median_eCPM, sh$median_eCPM)
t.test(fh$median_eCPM, th$median_eCPM)

