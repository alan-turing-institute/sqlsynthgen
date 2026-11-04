"""Generators write generator function definitions and queries into config.yaml."""

from collections.abc import Mapping

from sqlalchemy import MetaData

from datafaker.proposers.base import (
    ConstantProposerFactory,
    MultiProposerFactory,
    ProposerFactory,
)
from datafaker.proposers.choice import ChoiceProposerFactory
from datafaker.proposers.continuous import (
    ContinuousDistributionProposerFactory,
    ContinuousLogDistributionProposerFactory,
    MultivariateLogNormalProposerFactory,
    MultivariateNormalProposerFactory,
)
from datafaker.proposers.extract import DateComponentExtractProposerFactory
from datafaker.proposers.intervals import CopyProposerFactory, DateAfterProposerFactory
from datafaker.proposers.mimesis import (
    MimesisDateProposerFactory,
    MimesisDateTimeProposerFactory,
    MimesisFloatProposerFactory,
    MimesisIntegerProposerFactory,
    MimesisStringProposerFactory,
    MimesisTimeProposerFactory,
)
from datafaker.proposers.partitioned import (
    NullPartitionedLogNormalProposerFactory,
    NullPartitionedNormalProposerFactory,
)
from datafaker.proposers.sequence import IncrementProposerFactory


def everything_factory(config: Mapping, metadata: MetaData) -> ProposerFactory:
    """
    Get a factory that encapsulates all the other factories.

    :param config: The ``config.yaml`` configuration.
    :param metadata: The metadata of the source database.
    :return: A factory that is capable of returning any applicable proposers.
    """
    return MultiProposerFactory(
        MimesisStringProposerFactory(),
        MimesisIntegerProposerFactory(),
        MimesisFloatProposerFactory(),
        MimesisDateProposerFactory(),
        MimesisDateTimeProposerFactory(),
        MimesisTimeProposerFactory(),
        ContinuousDistributionProposerFactory(),
        ContinuousLogDistributionProposerFactory(),
        ChoiceProposerFactory(),
        ConstantProposerFactory(),
        MultivariateNormalProposerFactory(),
        MultivariateLogNormalProposerFactory(),
        NullPartitionedNormalProposerFactory(config, metadata),
        NullPartitionedLogNormalProposerFactory(config, metadata),
        DateAfterProposerFactory(config, metadata),
        CopyProposerFactory(config, metadata),
        DateComponentExtractProposerFactory(config, metadata),
        IncrementProposerFactory(),
    )
