from db.base import Base
from sqlalchemy import Column, ForeignKey, Integer, String, Float, DateTime, Time, UniqueConstraint, Boolean
import quandl
import pandas
import datetime
from config import QUANDL_API_KEY

class Series(Base):
	__tablename__ = 'Series'

	id = Column(Integer, primary_key=True)
	symbol = Column(String(5), nullable=False)
	ticker = Column(String(5), nullable=False)
	name = Column(String(100), nullable=False)
	type = Column(String(5), nullable=False)
	exchange = Column(String(100), nullable=False)
	quandl_code = Column(String(15), nullable=False)

	tick = Column(Float, nullable=True)
	cupp = Column(Float, nullable=True)
	delivery = Column(String(30), nullable=True)

	liq_start = Column(Time, nullable=True)
	liq_end = Column(Time, nullable=True)
	open_time = Column(Time, nullable=True)
	close_time = Column(Time, nullable=True)

	active = Column(Boolean, nullable=True)
	local_code = Column(Integer, nullable=True)
	currency = Column(String(5), nullable=True)
	tz = Column(String(50), nullable=True)

	def quandl_download(self, start_year=2000):
		upload = pandas.DataFrame()
		while start_year:
			for delivery_code in self.delivery:
				quandl_code = '{0}{1}{2}'.format(self.quandl_code, delivery_code, start_year)
				cusip = '{0}{1}{2}'.format(self.ticker, delivery_code, start_year)

				try:
					quandl_data = quandl.get(quandl_code, authtoken=QUANDL_API_KEY)
				except quandl.errors.quandl_error.NotFoundError:
					continue

				print(quandl_code)

				expiry_dt = quandl_data.iloc[-1].name

				#create contract
				contract = Contract(seriesid=self.id, cusip=quandl_code, expiry_dt=expiry_dt)
				contract.save()

				#need to rename volume column
				volume_column = quandl_data.columns[['volu' in col.lower() for col in quandl_data.columns]][0]
				quandl_data = quandl_data.rename(columns={volume_column:'Volume'})

				#pull daily data
				quandl_data['dt'] = pandas.to_datetime(quandl_data.index)
				quandl_data['contractid'] = contract.id
				quandl_data['contract_num'] = None
				quandl_data['open_'] = quandl_data.Open
				quandl_data['high'] = quandl_data.High
				quandl_data['low'] = quandl_data.Low
				quandl_data['close_'] = quandl_data.Settle
				quandl_data['volume'] = quandl_data.Volume
				quandl_data['chg'] = quandl_data.close_.diff(1)

				upload = upload.append(quandl_data)
			if start_year > datetime.datetime.now().year + 1:
				start_year = False
			else:
				start_year += 1

		upload = upload[['contractid', 'dt', 'contract_num', 'open_', 'high', 'low', 'close_', 'volume', 'chg']]
		upload_dict = upload.to_dict(orient='records')
		Daily.__table__.insert().execute(upload_dict)

		print('Finished')

class Contract(Base):
	__tablename__ = 'Contract'

	id = Column(Integer, primary_key=True)
	seriesid = Column(Integer, ForeignKey('Series.id'))
	cusip = Column(String(30), nullable=False)

	roll = Column(DateTime, nullable=True)
	first_notice = Column(DateTime, nullable=True)
	expiry_month = Column(Integer, nullable=True)
	expiry_dt = Column(DateTime, nullable=True)

	__table_args__ = (UniqueConstraint('seriesid', 'cusip'),
					)

	@property
	def series(self):
		#get series
		series = Series.filter(Series.id == self.seriesid).first()
		return series

class Daily(Base):
	__tablename__ = 'Daily'

	id = Column(Integer, primary_key=True)
	contractid = Column(Integer, ForeignKey('Contract.id'))
	dt = Column(DateTime, nullable=False)
	contract_num = Column(Integer, nullable=True)
	open_ = Column(Float, nullable=True)
	high = Column(Float, nullable=True)
	low = Column(Float, nullable=True)
	close_ = Column(Float, nullable=True)
	volume = Column(Integer, nullable=True)
	chg = Column(Float, nullable=True)
	atr10 = Column(Float, nullable=True)

	__table_args__ = (UniqueConstraint('dt', 'contractid'),)
