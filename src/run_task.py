import typer
from azure_logger import CsvLogger
from dep_tools.azure import get_container_client
from dep_tools.loaders import Sentinel2OdcLoader
from dep_tools.namers import DepItemPath
from dep_tools.runner import run_by_area
from dep_tools.s2_utils import S2Processor
from dep_tools.stac_utils import set_stac_properties
from dep_tools.writers import AzureDsWriter
from typing_extensions import Annotated
from xarray import DataArray

from grid import grid

BASE_PRODUCT = "s2"
DATASET_ID = "cloudless"
OUTPUT_NODATA = 0


class CloudlessProcessor(S2Processor):
    def process(self, xr: DataArray) -> DataArray:
        xr = super().process(xr)
        xr = xr.drop_sel(band="SCL")
        median = xr.median("time").compute().to_dataset("band")
        # rgba = median.odc.to_rgba(bands=["B04", "B03", "B02"], vmin=0, vmax=1000)
        return set_stac_properties(xr, median)


def main(
    region_code: Annotated[str, typer.Option()],
    region_index: Annotated[str, typer.Option()],
    datetime: Annotated[str, typer.Option()],
    version: Annotated[str, typer.Option()],
    dataset_id: str = DATASET_ID,
) -> None:
    cell = grid.loc[[(region_index, region_code)]]

    loader = Sentinel2OdcLoader(
        epsg=3857,
        datetime=datetime,
        dask_chunksize=dict(band=1, time=1, x=4096, y=4096),
        odc_load_kwargs=dict(
            fail_on_error=False,
            resolution=10,
            bands=["SCL", "B04", "B03", "B02"],
        ),
    )

    processor = CloudlessProcessor()
    itempath = DepItemPath(BASE_PRODUCT, dataset_id, version, datetime)

    writer = AzureDsWriter(
        itempath=itempath,
        overwrite=False,
        output_nodata=OUTPUT_NODATA,
        extra_attrs=dict(dep_version=version),
    )

    logger = CsvLogger(
        name=dataset_id,
        container_client=get_container_client(),
        path=itempath.log_path(),
        overwrite=False,
        header="time|index|status|paths|comment\n",
    )

    run_by_area(
        areas=cell,
        loader=loader,
        processor=processor,
        writer=writer,
        logger=logger,
        continue_on_error=False,
    )


if __name__ == "__main__":
    typer.run(main)
