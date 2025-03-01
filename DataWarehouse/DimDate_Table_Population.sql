-- Populate DimDate Table
DO $$
DECLARE
    StartDate DATE := '2000-01-01';
    EndDate DATE := '2050-12-31';
    CurrentDate DATE := StartDate;
BEGIN
    WHILE CurrentDate <= EndDate LOOP
        INSERT INTO DimDate (
            datest,
            date,
            year,
            quarter,
            month,
            day,
            dayofweek,
            isweekend,
            isholiday
        )
        VALUES (
            TO_CHAR(CurrentDate, 'YYYYMMDD')::INTEGER, -- datest (YYYYMMDD format)
            CurrentDate,                                -- date
            EXTRACT(YEAR FROM CurrentDate),             -- year
            EXTRACT(QUARTER FROM CurrentDate),          -- quarter
            EXTRACT(MONTH FROM CurrentDate),            -- month
            EXTRACT(DAY FROM CurrentDate),              -- day
            TO_CHAR(CurrentDate, 'Day'),               -- dayofweek
            EXTRACT(ISODOW FROM CurrentDate) IN (6, 7), -- isweekend (Saturday=6, Sunday=7)
            FALSE                                       -- isholiday (default to FALSE)
        );

        CurrentDate := CurrentDate + INTERVAL '1 day';
    END LOOP;
END $$;



-- Update for New Year's Day
UPDATE DimDate
SET isholiday = TRUE
WHERE month = 1 AND day = 1;

-- Update for Christmas
UPDATE DimDate
SET isholiday = TRUE
WHERE month = 12 AND day = 25;