from lodestar.database.presentation import WaterMarksView
from lodestar.apis.google import GoogleConfig


def update_watermarks_report():
    g = GoogleConfig()
    # g.update_sheet(sheet_name='sheet_name',
    #                data_object=WaterMarksView.data,
    #                include_update_time=True,
    #                update_time_offset=7)
