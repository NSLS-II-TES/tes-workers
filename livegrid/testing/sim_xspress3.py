from caproto.server import pvproperty, PVGroup, ioc_arg_parser, run
from textwrap import dedent


class SimulatedXspress3IOC(PVGroup):
    roi1_rbv = pvproperty(value=1, name="{{Xsp:1}}:C1_ROI1:Value_RBV")


if __name__ == '__main__':
    ioc_options, run_options = ioc_arg_parser(
        default_prefix='PV:XF:08BM-ES',
        desc="simulated Xspress3"
    )
    ioc = SimulatedXspress3IOC(**ioc_options)
    run(ioc.pvdb, **run_options)
