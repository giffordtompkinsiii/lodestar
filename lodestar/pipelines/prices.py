from . import *

class PricePipeline(AssetPipeline):
    unique_key = 'price_history_asset_id_date_key'

    def get_last_date(self):
        """Get last price record from the database."""
        current_price = self.asset.current_price
        if hasattr(current_price, 'date'):
            last_date = pd.to_datetime(current_price.date) or self.date_20y_ago
        else:
            last_date = self.date_20y_ago
        self.last_date = last_date
        logger.info(f"{self.asset.asset} last date: {self.last_date}")
        return last_date

    def __init__(self, asset: Asset, debug: bool):
        super().__init__(asset=asset, debug=debug)
        self.latest_price_date = self.get_last_date()
        self.price_data_start = pd.to_datetime(
                                dt.date(year=self.latest_price_date.year - 21, 
                                        month=self.latest_price_date.month, 
                                        day=self.latest_price_date.day))
        self.new_prices = None

    def get_prices(self, start_date: str = None, 
                   end_date: str = None, debug: bool = None) -> pd.DataFrame:
        """Pull prices from Yahoo!Finance for given `database.models.Asset`.
        
        Returns
        -------
        pandas.DataFrame: (asset_id, date, price)
            DataFrame of asset's price history.
        """
        a = self.asset
        begin_date = pd.to_datetime(start_date) or self.latest_price_date
        end_date = pd.to_datetime(end_date) or self.end_of_day
        history = yf.Ticker(a.asset.replace(' ','-')) \
                    .history(start=begin_date,
                             end=end_date, 
                             debug=(debug or self.debug))
        history = history[history.index > begin_date]
        logger.info(f"{a.asset} price records since last date: "
                    + f"{history.shape[0]}")

        if not a.price_history_collection and history.empty:
            logger.warning(f"No yfinance records or database records for "
                            + f"{a.asset}")
            return pd.DataFrame()
        
        logger.info("Sleeping for yFinance API.")
        time.sleep(1)

        import_df = history.reset_index()[['Date','Close']] \
                           .rename(columns={'Date':'date','Close':'price'})

        import_df['asset_id'] = a.id
        logger.debug(import_df)
        prices = import_df.itertuples(index=False)
        self.new_prices = [PriceHistory(**p._asdict()) for p in prices]
        return self.new_prices

    def run_prices(self, start_date: str = None, end_date: str = None, 
                         debug: bool = None) -> List[PriceHistory]:
        """Create and export new PriceHistory objects."""
        new_prices = self.get_prices(start_date, end_date, debug)
        on_conflict_do_nothing(new_prices, constraint_name=self.unique_key)
        session.refresh(asset)

    def __repr__(self):
        repr_str = f"{self.__class__.name}(Asset: '{self.asset.asset}', " \
                    + f"last_date: '{str(self.last_date.date())}', " \
                    + f"data_start': '{str(self.price_data_start.date())}', " \
                    + f"new_prices: {len(self.new_prices or [])} records, " \
                    + f"debug: {self.debug})"
        return repr_str


if __name__=='__main__':
    for asset in asset_map.values():
        p = PricePipeline(asset, True)
        p.run_prices()
