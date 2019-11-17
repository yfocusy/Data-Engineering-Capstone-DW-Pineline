from __future__ import division, absolute_import, print_function
from airflow.plugins_manager import AirflowPlugin

import operators
import helpers

# from plugins import operators
# from plugins import helpers

class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator,
        operators.LoadLookupOperator
    ]

    helpers = [
        helpers.SqlQueries
    ]

