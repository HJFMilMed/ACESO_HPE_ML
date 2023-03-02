select  DATEADD(MINUTE, DATEDIFF(MINUTE, 0, [idx]) / 360 * 360, 0)  as [idx],
		[patient_id],
       avg([heart-rate]) as [heart-rate],
	   avg([respiration-rate]) as [respiration-rate],
	   avg([count-of-magnitude]) as 	[count-of-magnitude],
	   avg([ecg-sqi]) as [ecg-sqi],
	   avg([gross-activity]) as [gross-activity],
	   avg([hrv-td]) as [hrv-td],
	   avg([magnitude-of-uni-counts]) as [magnitude-of-uni-counts],
	   avg([max-of-uni-counts]) as [max-of-uni-counts],
	   avg([percent-afib]) as [percent-afib],
	   avg([percent-asleep]) as [percent-asleep],
	   avg([posture]) as posture,
	   avg([step-count]) as [step-count],
	   avg([tilt]) as [tilt],
	   avg([trailing-activity]) as [trailing-activity],
	   avg([uni-count-x]) as [uni-count-x],
	   avg([uni-count-y]) as [uni-count-y],
	   avg([uni-count-z]) as [uni-count-z],
	   avg([walk-percent]) as [walk-percent]
from [master].[dbo].[physiq_id_timestamp_fullload]
group by  DATEDIFF(MINUTE, 0, [idx]) / 360,patient_id
order by idx desc