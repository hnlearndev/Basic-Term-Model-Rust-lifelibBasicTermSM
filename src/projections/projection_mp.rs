use crate::assumptions::assumption_scenario::AssumptionScenario;
use chrono::NaiveDate;
use ndarray::Array1;
use polars::prelude::*;

mod asl_se_model;
mod s_model;
mod se_model;

use self::{asl_se_model::ASLSEModelPoint, s_model::SModelPoint, se_model::SEModelPoint};

pub enum ModelPoint {
    SModel(SModelPoint),
    SEModel(SEModelPoint),
    ASLSEModel(ASLSEModelPoint),
}

impl ModelPoint {
    pub fn project(&self, assumptions: &AssumptionScenario) -> PolarsResult<LazyFrame> {
        match self {
            ModelPoint::SModel(mp) => mp.project(assumptions),
            ModelPoint::SEModel(mp) => mp.project(assumptions),
            ModelPoint::ASLSEModel(mp) => mp.project(assumptions),
        }
    }
}
