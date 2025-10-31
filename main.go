package main

/*
Описание
Нам нужно передать данные из некоторого источника некоторому потребителю. При этом источник отдает данные небольшими пачками (~ десятки записей), а потребитель оптимальнее работает с крупными батчами (~ тысячи записей). Реальный пример - поставка данных из очередей типа Kafka в базу Clickhouse.
Источник и потребитель описываются интерфейсами:
*/

/*

Источник:
Условно бесконечный.
Источник никогда не возвращает более `MaxItems` записей за один вызов `Next`.
В рамках одной "сессии" (одного вызова функции `Pipe`) источник каждый раз возвращает новые данные на каждый вызов `Next`.
Однако, после перезапуска источник начнет с прошлой "подтвержденной" позиции, задаваемой `cookie`.
Поэтому *каждое* значение `cookie`, которое вернул вызов `Next`, после сохранения данных в приемнике,
должно быть фиксировано вызовом `Commit`, причем строго в той же последовательности, в которой их вернул `Next`
Приемник:
Не может обработать более `MaxItems` за один раз.
Базовая задача:
Требуется реализовать функцию:
для Go: func Pipe(p Producer, c Consumer) error ,
Которая читает данные из источника, группирует их в буфер размером не более `MaxItems` и сохраняет в приёмник,
после чего фиксирует прогресс в источнике.
*/

const MaxItems = 10000

type Producer interface {
	// Next returns:
	// - batch of items to be processed
	// - cookie to be commited when processing is done
	// - error
	Next() (items []any, cookie int, err error)
	// Commit is used to mark data batch as processed
	Commit(cookie int) error
}

type Consumer interface {
	Process(items []any) error
}

// 3000
// 3000
// 3000
// 3000 либо обработать 9000, либо 12000, либо 10000 => обработать 9000

func Pipe(p Producer, c Consumer) error {
	// 1 - Создаём слайс с капасити MaxItems - буфер, и слайс для cookie
	// 2 - Наполняем его пачками проверяя текущую длину и MaxItems-что осталось из cap-len (в цикле) + накапливаем cookie
	// * внимательно обработать кейс с 3000 выше
	// 3 - Вызываем Process
	// 4 - Коммитим имеющиеся данные (в цикле - может быть накоплено несколько cookie)
	// 5 - Продолжаем принимать данные

	// Слайс для батчей
	buffer := make([]any, 0, MaxItems)
	// Слайс для куки
	var cookies []int
	// Считаем полученные батчи (чтобы не перевалить за капасити)
	var totalBatches int
	// Считаем полученные куки, чтобы корректно их закоммитить
	var totalCookies int

	for {
		items, cookie, err := p.Next()
		if err != nil {
			return err
		}
		if len(items) == 0 {
			if len(buffer) > 0 {
				if err := c.Process(buffer); err != nil {
					return err
				}
				for _, v := range cookies {
					if err := p.Commit(v); err != nil {
						return err
					}
				}
			}
			break
		} else if (MaxItems - len(buffer)) < len(items) {
			// Случай, когда мы отправляем уже накопленные данные
			err := c.Process(buffer)
			if err != nil {
				return err
			}
			for _, v := range cookies {
				err := p.Commit(v)
				if err != nil {
					return err
				}
			}
			// Обнуляем оба слайса
			buffer = buffer[:0]
			cookies = cookies[:0]
			// Наполняем теми данными, что не влезли
			buffer = append(buffer, items...)
			cookies = append(cookies, cookie)
			totalBatches += len(items)
			totalCookies += 1
		} else if (MaxItems - len(buffer)) > len(items) {
			// Просто добавляем в буфер, т.к. есть ещё место
			buffer = append(buffer, items...)
			cookies = append(cookies, cookie)
			totalBatches += len(items)
			totalCookies += 1
		} else {
			// Пограничный случай, когда равно, добавляем и отправляем и коммитим сразу
			buffer = append(buffer, items...)
			cookies = append(cookies, cookie)
			totalBatches += len(items)
			totalCookies += 1
			// Отправляем, коммитим
			err := c.Process(buffer)
			if err != nil {
				return err
			}
			for _, v := range cookies {
				err := p.Commit(v)
				if err != nil {
					return err
				}
			}
			// Обнуляем оба слайса
			buffer = buffer[:0]
			cookies = cookies[:0]
		}

	}

	return nil
}
