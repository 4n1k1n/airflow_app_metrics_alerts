<h1>Система алертов приложения: лента новостей и мессенджер</h1>

<p>Система каждые 15 минут проверяет ключевые метрики – активные пользователи в ленте / мессенджере, просмотры, лайки, CTR, количество отправленных сообщений.</p>
<p>Для выгрузки метрик выполняется запрос в ClickHouse, используется библиотека pandahouse.</p>
<p>В случае отклонения метрики отправляется информация в телеграм чат.</p>
<h2>Структура данных в ClickHouse</h2>
<h3>Таблица feed_actions</h3>
<table>
  <thead>
    <tr>
      <th>user_id</th>
      <th>post_id</th>
      <th>action</th>
      <th>time</th>
      <th>gender</th>
      <th>age</th>
      <th>country</th>
      <th>city</th>
      <th>os</th>
      <th>source</th>
      <th>exp_group</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>139605</td>
      <td>2243</td>
      <td>view</td>
      <td>07/03/24 00:00</td>
      <td>1</td>
      <td>23</td>
      <td>Russia</td>
      <td>Birsk</td>
      <td>iOS</td>
      <td>organic</td>
      <td>0</td>
    </tr>
    <tr>
      <td>138505</td>
      <td>2436</td>
      <td>view</td>
      <td>07/03/24 00:00</td>
      <td>0</td>
      <td>24</td>
      <td>Russia</td>
      <td>Rostov</td>
      <td>Android</td>
      <td>organic</td>
      <td>1</td>
    </tr>
    <tr>
      <td>140314</td>
      <td>2436</td>
      <td>like</td>
      <td>07/03/24 00:00</td>
      <td>1</td>
      <td>19</td>
      <td>Russia</td>
      <td>Yelizovo</td>
      <td>Android</td>
      <td>organic</td>
      <td>3</td>
    </tr>
  </tbody>
</table>

<h3>Таблица message_actions</h3>
<table>
  <thead>
    <tr>
      <th>user_id</th>
      <th>receiver_id</th>
      <th>time</th>
      <th>source</th>
      <th>exp_group</th>
      <th>gender</th>
      <th>age</th>
      <th>country</th>
      <th>city</th>
      <th>os</th>     
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>112866</td>
      <td>110282</td>
      <td>07/03/24 00:00</td>
      <td>organic</td>
      <td>3</td>
      <td>1</td>
      <td>29</td>
      <td>Russia</td>
      <td>Moscow</td>
      <td>Android</td>
    </tr>
    <tr>
      <td>111607</td>
      <td>113653</td>
      <td>07/03/24 00:00</td>
      <td>organic</td>
      <td>1</td>
      <td>1</td>
      <td>20</td>
      <td>Russia</td>
      <td>Barnaul</td>
      <td>Android</td>
    </tr>
    <tr>
      <td>7401</td>
      <td>7800</td>
      <td>07/03/24 00:00</td>
      <td>ads</td>
      <td>0</td>
      <td>0</td>
      <td>17</td>
      <td>Azerbaijan</td>
      <td>Ağdaş</td>
      <td>iOS</td>
    </tr>
  </tbody>
</table>
<h2>Принцип проверки:</h2>
<ul>
  <li>Отбираются 5 значений метрики за последние 7 дней:</li>
  <ul>
    <li>Для каждого дня две 15-ти минутки до текущего времени + текущее время + две 15-ти минутки после текущего времени.</li>
    <li>В текущий день будут отобраны значения до текущего времени (3 значения).</li>
  </ul>
  <li>Внутри каждого дня отбираются значения в интервале ±2σ, чтобы исключить влияние прошлых выбросов.</li>
  <li>Для отобранных значений считается доверительный интервал ±3σ.</li>
  <li>Если последнее значение выходит за пределы этого интервала – отправляется alert в телеграм.</li>
</ul>
<h2>Пример алерта в телеграм</h2>
<p>Сообщение:</p>
<blockquote>
  <p>
  05-05-2024 03:30
  <br>
  Метрика likes
  <br>
  Текущее значение: 380.0
  <br>
  Границы доверительного интервала: (436.93 - 837.57)
  <br>
  Ссылка на дашборд:
  <br>
  http://site
  </p>
</blockquote>
<p>График изменения метрики за последние 48 часов:</p>
<img src="plot_example.jpeg">
