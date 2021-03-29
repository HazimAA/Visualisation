Carrying out Rolling Retention in Oracle SQL, using a reference point.

-----------------------
Step 1: Create Dataset:  
-----------------------

```
CREATE TABLE logins(
user_id varchar2(250),
login_time int
)
;


INSERT INTO logins (user_id, login_time) VALUES ('a005baae', 1462147200);
INSERT INTO logins (user_id, login_time) VALUES ('a005baae', 1462752000);
INSERT INTO logins (user_id, login_time) VALUES ('a03224fb', 1462147200);
INSERT INTO logins (user_id, login_time) VALUES ('a03224fb', 1462752000);
INSERT INTO logins (user_id, login_time) VALUES ('a03224fb', 1463356800);
INSERT INTO logins (user_id, login_time) VALUES ('a05e88ec', 1462752000);
INSERT INTO logins (user_id, login_time) VALUES ('a05e88ec', 1463356800);
INSERT INTO logins (user_id, login_time) VALUES ('a05e9452', 1462752000);
```

--------------------------------
Step 2: Bucketing Visits By Week
--------------------------------

You need to group by login time to squash all of the user logins together
```
SELECT
    user_id,
    TRUNC(TO_DATE('19700101','yyyymmdd') + (login_time/24/60/60),'DAY') AS login_week
FROM logins
ORDER BY user_id, login_week
;
```

--------------------------
Step 3: Normalizing Visits
--------------------------

The next step is to calculate the number of weeks between the week of the first visit and the given visits’ week.
```
WITH by_week AS
(
SELECT
    user_id,
    TRUNC(TO_DATE('19700101','yyyymmdd') + (login_time/24/60/60),'DAY') AS login_week
FROM logins
order by user_id, login_week
),
with_first_week AS
(
SELECT
  user_id,
  login_week,
  FIRST_VALUE(login_week) OVER (PARTITION BY user_id ORDER BY login_week) AS first_week
FROM by_week
)

SELECT
  user_id,
  login_week,
  first_week,
  (login_week - first_week)/7 AS week_number
FROM with_first_week
;
```

-------------------
Step 4: Tallying Up
-------------------

We are almost done. Now, we need to create the “pivot table” view of retention analysis.

```
WITH by_week AS
(
SELECT
    user_id,
    TRUNC(TO_DATE('19700101','yyyymmdd') + (login_time/24/60/60),'DAY') AS login_week
FROM logins
order by user_id, login_week
),
with_first_week AS
(
SELECT
  user_id,
  login_week,
  FIRST_VALUE(login_week) OVER (PARTITION BY user_id ORDER BY login_week) AS first_week
FROM by_week
),
with_week_number AS
(
SELECT
  user_id,
  login_week,
  first_week,
  (login_week - first_week)/7 AS week_number
FROM with_first_week
)

SELECT
  first_week,
  SUM(CASE WHEN week_number = 0 THEN 1 ELSE 0 END) AS week_0,
  SUM(CASE WHEN week_number = 1 THEN 1 ELSE 0 END) AS week_1,
  SUM(CASE WHEN week_number = 2 THEN 1 ELSE 0 END) AS week_2,
  SUM(CASE WHEN week_number = 3 THEN 1 ELSE 0 END) AS week_3,
  SUM(CASE WHEN week_number = 4 THEN 1 ELSE 0 END) AS week_4,
  SUM(CASE WHEN week_number = 5 THEN 1 ELSE 0 END) AS week_5,
  SUM(CASE WHEN week_number = 6 THEN 1 ELSE 0 END) AS week_6,
  SUM(CASE WHEN week_number = 7 THEN 1 ELSE 0 END) AS week_7,
  SUM(CASE WHEN week_number = 8 THEN 1 ELSE 0 END) AS week_8,
  SUM(CASE WHEN week_number = 9 THEN 1 ELSE 0 END) AS week_9
FROM with_week_number
GROUP BY first_week
ORDER BY first_week
;
```
