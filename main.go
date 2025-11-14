package main

import (
	"context"
	"sync"
)

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
	Next(ctx context.Context) (items []any, cookie int, err error) // Добавил контекст
	// Commit is used to mark data batch as processed
	Commit(ctx context.Context, cookie int) error // Добавил контекст
}

type Consumer interface {
	Process(ctx context.Context, items []any) error // Добавил контекст
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

	// -----------------------------------------------------------------------------------------------------------------

	// Слайс для батчей
	buffer := make([]any, 0, MaxItems)
	// Слайс для куки
	var cookies []int
	// Добавил структуру, которую будем передавать в канал (сразу и слайс данных и куки, которые надо закоммитить)
	type batch struct {
		items  []any
		cookie []int
	}
	// Канал, через который будем передавать батчи из продюссера в консюмер
	butchCh := make(chan batch, 3) // Добавил небольшой буфер для подстраховки
	// Ошибка для возврата из функции
	var firstError error
	// Новый подход к обработке первой ошибки
	var errOnce sync.Once
	// wg для наших горутин
	var wg sync.WaitGroup
	// Контекст для отмены по ошибке
	ctx, cancel := context.WithCancel(context.Background())
	/*
		Нашёл интересную вещь по завершению, можно сделать контекст через:
		ctx, cancel := signal.NotifyContext(
			context.Background(),
			syscall.SIGTERM,
			syscall.SIGHUP,
			syscall.SIGINT,
			syscall.SIGQUIT,
		)
		Возможно такой вариант будет даже чуть интереснее простого контекста с отменой.

	*/

	// -----------------------------------------------------------------------------------------------------------------

	/*
		Так, надо поделить код на две горутины:
		1) Читаем источник через Next(), соединяем данные и проверяем длину. Когда надо - пишем наш батч в канал.
		2) Ждёт данные из канала, когда получает - запускаем Process() и Commit().
	*/

	// 1-ая горутина
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(butchCh)

		for {
			if ctx.Err() != nil {
				// Перед выходом отправим, что накопилось
				return
			}

			items, cookie, err := p.Next(ctx)

			// Тут теперь не просто проверяем на ошибку, а пишем её в переменную firstError, которую вернём из функции
			// и отменяем контекст (теперь через sync.Once)
			if err != nil {
				errOnce.Do(func() {
					firstError = err
					cancel()
				})
				return
			}

			// Если источник пустой, просто продолжаем
			if len(items) == 0 {
				continue
			}

			// Если не влезаем, то пишем наши слайсы в структуру батча и кладём её в канал
			if (MaxItems - len(buffer)) < len(items) {
				select {
				case <-ctx.Done():
					return
				case butchCh <- batch{items: buffer, cookie: cookies}:
				}
				buffer = buffer[:0]
				cookies = cookies[:0]
			}

			buffer = append(buffer, items...)
			cookies = append(cookies, cookie)

		}
	}()

	// 2-ая горутина
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				return
			case b, ok := <-butchCh:
				if !ok {
					return
				}
				if err := c.Process(ctx, b.items); err != nil {
					errOnce.Do(func() {
						firstError = err
						cancel()
					})
					return
				}
				for _, c := range b.cookie {
					if err := p.Commit(ctx, c); err != nil {
						errOnce.Do(func() {
							firstError = err
							cancel()
						})
						return
					}
				}
			}
		}
	}()

	wg.Wait()
	return firstError
}
