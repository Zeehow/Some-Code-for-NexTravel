CREATE TEMPORARY TABLE ids 
(SELECT distinct cc.companyId as ci, c.name as name, c.size as size, count(o.orderId) as orders,
cc.industryLevel0 as industryLevel0, cc.industryLevel1 as industryLevel1, cc.industryLevel2 as industryLevel2
from nex.clearbit_company cc
left join nex.company c on c.companyId = cc.companyId
inner join nex.order o on o.companyId = c.companyId
group by ci);

CREATE temporary table id2(
select c.companyId as cid, count(e.emailId) as emails
from nex.email as e
left join nex.user as u on e.emailFrom = u.email
left join nex.company as c on c.companyId = u.companyId
group by cid
);

select i.*, count(p.phoneId) as calls, u.userId, id.emails
from ids as i
left join nex.user as u on u.companyId = i.ci
left join nex.phone as p on u.userId = p.userId
left join id2 as id on u.companyId = id.cid
where i.ci != -1 and isnull(i.name) != 1
group by i.ci;

drop table ids;
drop table id2;