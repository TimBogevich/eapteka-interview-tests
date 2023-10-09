-- Напишите SQL который селектит таблицу sales и добавляет поле is_last_operation с флагом последней операции клиента

SELECT
	datetime,
	client_id,
	order_id,
	item_id,
	amount,
	"cost",
	vat,
	return_flag,
	CASE
		WHEN MAX(datetime) OVER (
			PARTITION BY client_id
		) = datetime THEN TRUE
		ELSE FALSE
	END AS is_last_operation
FROM
	sales
ORDER BY
	client_id,
	datetime

-- Напишите DQ метрики для которые могут использоваться для этой таблицы

-- DQ метрики могут быть следующими: проверка данных на NULL поля, проверка данных на соответствие ячейки столбцу (int to int, bool to bool, etc.), 
-- наличие обязательных полей, должно быть выставлено корректное время, float поля должны быть корректными, необходимо чтобы ключевые поля по типу 
-- id были консистентными с уже имеющимися данными (тип и формат id должен быть согласован, без повторений), следует проверить данные на адекватность 
-- и выбросы (например, в первоисточнике ошиблись и не поставили запятую в cost, а значит мы получим товар не за 44,50, а за 4450,0).


-- Вывести email, которые встречаются в таблице больше 1 раза.

SELECT
	email
FROM
	(
		SELECT
			email,
			COUNT(email) AS email_cnt
		FROM
			public.сlient AS с
		GROUP BY
			email
	) AS t
WHERE
	email_cnt > 1


-- Вывести идентификаторы клиентов у которых email встречается в таблице больше 1 раза.
	
WITH repeatable_emails AS (
	SELECT
		email
	FROM
		(
			SELECT
				email,
				COUNT(email) AS email_cnt
			FROM
				public.сlient AS с
			GROUP BY
				email
		) AS t
	WHERE
		email_cnt > 1
)

SELECT
	id,
	email
FROM
	public.сlient AS с
WHERE
	email IN (
		SELECT
			email
		FROM
			repeatable_emails AS r
	)
