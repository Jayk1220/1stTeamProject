CREATE TABLE RISK (
    ID         NUMBER(38,0),
    RDATE      DATE,
    INDUSTRY   VARCHAR2(50),
    MEAN_SENT  FLOAT,
    RISK       FLOAT,
    PREDICT    FLOAT,
    total_news              NUMBER(10,0)   NOT NULL,
    article_ratio           NUMBER(6,5)    NOT NULL,
    total_volume            NUMBER(19,0)   NOT NULL, 
    trade_volume_ratio      NUMBER(6,5)    NOT NULL
);
CREATE TABLE STOCK (
    ID            NUMBER(38,0),
    SDATE         DATE,
    MARKET_INDEX  VARCHAR2(50),
    CLOSE         FLOAT,
    CHANGE        FLOAT,
    VOLUME        NUMBER(38,0)
);
DROP TABLE RISK;
DROP TABLE STOCK;

SELECT 
TO_CHAR(MIN(SDATE), 'YYYY-MM-DD') AS min_date,
TO_CHAR(MAX(SDATE), 'YYYY-MM-DD') AS max_date
FROM STOCK;