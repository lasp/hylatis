package latis.input

import latis.dataset.Dataset

trait StaticDatasetResolver extends DatasetResolver {

  def datasetIdentifier: String

  def getDataset(): Dataset

  def getDataset(id: String): Option[Dataset] = {
    if (datasetIdentifier == id) Option(getDataset())
    else None
  }
}
