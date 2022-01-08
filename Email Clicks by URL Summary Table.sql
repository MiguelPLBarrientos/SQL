/*
The ask for this project was to retrieve the number of clicks on links in
specific emails. PostGre SQL conventions used.
*/

select distinct

	/* Email Metadata */
	sj.sendid as "Send ID",
	sj.senddate as "Send Date",
	sj.emailname as "Email Name",
	UPPER(c.alias) as "URL Alias",
	MAX(c.url) as url,

	/* Total Clicks */
	COUNT(c.clicks_id) as "Total Clicks",
	COUNT(c.clicks_id)::decimal / MAX(c2.total_clicks) as "% of Total Clicks",
	COUNT(c.clicks_id)::decimal / MAX(s2.num_deliveries) as "Total CTR %",

	/* Unique Clicks */
	COUNT(distinct c.subscriberid) as "Unique Clicks",
	COUNT(distinct c.subscriberid)::decimal / MAX(c2.unique_clicks) as "% of Unique Clicks",
	COUNT(distinct c.subscriberid)::decimal / MAX(s2.num_deliveries) as "Unique CTR %"

/*
This table contains one row for each email sent out. Think of it like a
master list of all emails we have sent out.
*/

from sendjobs sj

/* The clicks table contains a list of every click ever made. */
left join clicks c
	on sj.sendid = c.sendid

/* I created this second clicks table in order to create the %s easier */
left join (
		select
			sendid,
			count(clicks_id) as total_clicks,
			count(distinct subscriberid) as unique_clicks
		from fact_clicks
		group by sendid
	) c2
	on sj.sendid = c2.sendid

/*
The sends table contains a list of every email sent. The main difference between
the sendjobs table and the sends table is that he same email will have multiple
emails. The sends table has every send, the sendjobs table has 1 row for every
email. The bounces table is a list of every email that "bounces" or doesn't reach
the user's inbox. These two tables are important for determining how many
deliveries there were. This table is used for determining "click thru rate",
or the number of people clicked on the email/link
*/
left join (
		select
			a.sendid,
			count(distinct a.subscriberid) as num_sends,
			count(distinct b.subscriberid) as num_bounces,
			count(distinct a.subscriberid) - count(distinct b.subscriberid) as num_deliveries
		from sends a
		left join bounces b
			on a.sendid = b.sendid
		group by a.sendid
	) s2
	on sj.sendid = s2.sendid

/*
Here is a list of ever email we were looking for. This is fake for the sample.
*/
where
	-- Some emails we knew the exact id
	sj.sendid in (1, 2, 3, 4, etc.)
	-- Other we needed to use the email name or the day it was sent
	or sj.emailname like '%Email 1%'
	or (sj.emailname like '%Email 2%' and sj.senddate = '2021-11-30')

group by
	sj.sendid,
	sj.senddate,
	sj.emailname,
	UPPER(c.alias)

order by
	sj.sendid,
	sj.senddate asc,
	COUNT(c.clicks_id) desc
;
