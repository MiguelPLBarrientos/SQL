/*
The ask for this project was to create a summary table for each email
subscriber so we could further analyze trends in Python. We were hoping to do
"research" on things like what leads to unsubscribes and to create segments
of subscribers based on their email behaviour. PostGre SQL conventions used.
*/

/* -------------------------------------------------------------------- */
--Dropping tables created in this script from the database to start
drop table if exists
	subscriber_summary_table,
	subscriber_summary_table_v2,
	tenure_table,
	sends_tenure_table,
	num_sends_ever_table,
	num_sends_p30d_table,
	num_sends_p90d_table,
	num_opens_ever_table,
	num_opens_p30d_table,
	num_opens_p90d_table,
	num_clicks_ever_table,
	num_clicks_p30d_table,
	num_clicks_p90d_table,
	bounces_table,
	unsubs_table;

/* Get initial tables */
/* Tenure */
select
	subscriberid,
	status,
	datecreated,
	dateunsubscribed,
	case
		when status = 'unsub' then dateunsubscribed - datecreated
		else current_date - datecreated
	end as tenure
into tenure_table
from subscribers ds;

/* Table for gathering contact age and last email send */
select
	subscriberid,
	MIN(eventdate) as earliest_send,
	MAX(eventdate) as latest_send,
	datediff('day', earliest_send, getdate()) as earliest_contact_age_days,
	datediff('day', latest_send, getdate()) as last_contact_age_days
into sends_tenure_table
from sends s2 group by subscriberid;



/* ------------------------ */
/* Sends */
/* ------------------------ */

/* Get # of emails ever sent by subscriberid */
select
	subscriberid,
	COUNT(distinct sendid) as num_sends_ever
into num_sends_ever_table
from sends s2
group by subscriberid;

/* Get # of emails sent in past 30 days by subscriberid */
select
	subscriberid,
	COUNT(distinct sendid) as num_sends_p30d
into num_sends_p30d_table
from sends s2
where datediff('day', eventdate, getdate()) <= 30
group by subscriberid;

/* Get # of emails sent in past 90 days by subscriberid */
select
	subscriberid,
	COUNT(distinct sendid) as num_sends_p90d
into num_sends_p90d_table
from sends s2
where datediff('day', eventdate, getdate()) <= 90
group by subscriberid;


/* ------------------------ */
/* Clicks */
/* ------------------------ */

/* Get # of emails ever clicked on by subscriber id */
select
	subscriberid,
	COUNT(distinct clicks_id) as num_clicks_ever,
	COUNT(distinct sendid) as num_uclicks_ever
into num_clicks_ever_table
from clicks c
group by subscriberid;

/* Get # of email clicks in past 30 days by subscriberid */
select
	subscriberid,
	COUNT(distinct clicks_id) as num_clicks_p30d,
	COUNT(distinct sendid) as num_uclicks_p30d
into num_clicks_p30d_table
from clicks c
where datediff('day', eventdate, getdate()) <= 30
group by subscriberid;


/* Get # of email clicks in past 90 days by subscriberid */
select
	subscriberid,
	COUNT(distinct clicks_id) as num_clicks_p90d,
	COUNT(distinct sendid) as num_uclicks_p90d
into num_clicks_p90d_table
from clicks c
where datediff('day', eventdate, getdate()) <= 90
group by subscriberid;

/* ------------------------ */
/* Opens */
/* ------------------------ */

/* Get # of emails ever opened by subscriberid */
select
	subscriberid,
	COUNT(distinct opens_id) as num_opens_ever,
	COUNT(distinct sendid) as num_uopens_ever
into num_opens_ever_table
from opens o
group by subscriberid;

/* Get # of email opens in past 30 days by subscriberid */
select
	subscriberid,
	COUNT(distinct opens_id) as num_opens_p30d,
	COUNT(distinct sendid) as num_uopens_p30d
into num_opens_p30d_table
from opens o
where datediff('day', eventdate, getdate()) <= 30
group by subscriberid;


/* Get # of email opens in past 90 days by subscriberid */
select
	subscriberid,
	COUNT(distinct opens_id) as num_opens_p90d,
	COUNT(distinct sendid) as num_uopens_p90d
into num_opens_p90d_table
from opens o
where datediff('day', eventdate, getdate()) <= 90
group by subscriberid;


/* ------------------------ */
/* Bounces */
/* ------------------------ */

/* Gets number of bounces, latest bounce on the account, and the reason why */
select
	fb.subscriberid,
	fb3.num_bounces_ever,
	fb3.num_ubounces_ever,
	fb4.num_bounces_p30d,
	fb4.num_ubounces_p30d,
	fb5.num_bounces_p90d,
	fb5.num_ubounces_p90d,
	fb.eventdatetime as Latest_Bounce_Date,
	fb.bouncecategory
into bounces_table
from bounces b
left join (
    select subscriberid, max(eventdatetime) as MaxDate
    from bounces
    group by subscriberid
	) b2
	on fb.subscriberid = fb2.subscriberid and fb.eventdatetime = fb2.MaxDate

left join (
    select
    	subscriberid,
    	count(distinct bounces_id) as num_bounces_ever,
    	count(distinct sendid) as num_ubounces_ever
    from bounces
    group by subscriberid
	) b3
	on fb.subscriberid = fb3.subscriberid

left join (
    select
    	subscriberid,
    	count(distinct bounces_id) as num_bounces_p30d,
    	count(distinct sendid) as num_ubounces_p30d
    from bounces
    where datediff('day', eventdate, getdate()) <= 30
    group by subscriberid
	) b4
	on fb.subscriberid = fb4.subscriberid


left join (
    select
    	subscriberid,
    	count(distinct bounces_id) as num_bounces_p90d,
    	count(distinct sendid) as num_ubounces_p90d
    from bounces
    where datediff('day', eventdate, getdate()) <= 90
    group by subscriberid
	) b5
	on fb.subscriberid = fb5.subscriberid;

/* ------------------------ */
/* Unsubscribes */
/* ------------------------ */
/* Get unsubscribe metrics */
select
	subscriberid,
	case
		when COUNT(distinct unsubs_id) >= 1 then 1
		else 0
	end as ever_unsubbed_flag,
	COUNT(distinct unsubs_id) as num_unsubs_ever,
	COUNT(distinct sendid) as num_uunsubs_ever
into unsubs_table
from unsubs
group by subscriberid;

/* ------------------------ */
/* Final summary table without average tables or old segment */
/* ------------------------ */

select
	a.subscriberid,
	b.status,
	b.tenure,
	c.earliest_send,
	c.earliest_contact_age_days,
	c.latest_send,
	c.last_contact_age_days,
	d.num_sends_ever,
	e.num_sends_p30d,
	f.num_sends_p90d,
	g.num_clicks_ever,
	g.num_uclicks_ever,
	h.num_clicks_p30d,
	h.num_uclicks_p30d,
	i.num_clicks_p90d,
	i.num_uclicks_p90d,
	j.num_opens_ever,
	j.num_uopens_ever,
	k.num_opens_p30d,
	k.num_uopens_p30d,
	l.num_opens_p90d,
	l.num_uopens_p90d,
	m.num_bounces_ever,
	m.num_ubounces_ever,
	m.num_bounces_p30d,
	m.num_ubounces_p30d,
	m.num_bounces_p90d,
	m.num_ubounces_p90d,
	m.Latest_Bounce_Date,
	m.bouncecategory,
	n.ever_unsubbed_flag,
	n.num_unsubs_ever,
	n.num_uunsubs_ever
into subscriber_summary_table
from subscribers a

/* Tenure */
left join tenure_table b
	on a.subscriberid = b.subscriberid

/* Table for gathering contact age and last email send */
left join sends_tenure_table c
	on a.subscriberid = c.subscriberid


/* Sends */
left join num_sends_ever_table d
	on a.subscriberid = d.subscriberid

left join num_sends_p30d_table e
	on a.subscriberid = e.subscriberid

left join num_sends_p90d_table f
	on a.subscriberid = f.subscriberid

/* Clicks */
left join num_clicks_ever_table g
	on a.subscriberid = g.subscriberid

left join num_clicks_p30d_table h
	on a.subscriberid = h.subscriberid

left join num_clicks_p90d_table i
	on a.subscriberid = i.subscriberid

/* Opens */
left join num_opens_ever_table j
	on a.subscriberid = j.subscriberid

left join num_opens_p30d_table k
	on a.subscriberid = k.subscriberid

left join num_opens_p90d_table l
	on a.subscriberid = l.subscriberid

/* Gets number of bounces, latest bounce on the account, and the reason why */
left join bounces_table m
	on a.subscriberid = m.subscriberid

/* Get unsubscribe metrics */
left join unsubs_table n
	on a.subscriberid = n.subscriberid;

/* -------------------------------------------------------------------- */

/* Final Segment Summary Table ->
 * Join onto average tables */
select
	a.*,
	b.avg_diff_sends_hours,
	c.avg_send_click_diff_hours

--into subscriber_summary_final /* For whatever reason this results in an "interval" error */
from subscriber_summary_table a

/* Average Difference between Sends */
left join (
	select
		subscriberid,
		avg(difference_previous_send) as avg_diff_sends_hours
	from (select
			subscriberid,
			sendid,
			eventdatetime,
			LAG(eventdatetime) OVER (PARTITION BY subscriberid ORDER BY eventdatetime) AS previous_send,
			datediff('hour', LAG(eventdatetime) OVER (PARTITION BY subscriberid ORDER BY eventdatetime), eventdatetime) AS difference_previous_send
		from sends s2)
	group by subscriberid) b
on a.subscriberid = b.subscriberid


/* Average difference between send and click */
left join (
	select
		subscriberid,
		avg(send_click_diff) as avg_send_click_diff_hours
	from (select
			fc.subscriberid,
			fc.sendid,
			fc.clicks_id,
			fs2.eventdatetime as send_date,
			fc.eventdatetime as click_date,
			datediff('hour', fs2.eventdatetime, fc.eventdatetime)  as send_click_diff
		from clicks c
		left join sends s2
			on fc.subscriberid = fs2.subscriberid
			and fc.sendid = fs2.sendid
		where fc.subscriberid in
			(select distinct subscriberid from fact_clicks)
		order by subscriberid, fs2.eventdatetime desc)
	group by subscriberid) c
on a.subscriberid = c.subscriberid
where
	a.earliest_contact_age_days is not null
order by subscriberid;


/* Export to CSV */

/* Run models */

/* Assign segments */

/* Get current segment, compare vs. new segments */
