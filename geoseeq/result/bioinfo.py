from geoseeq.constants import FASTQ_MODULE_NAMES


class SampleBioInfoFolder:
    """Abstract class that adds bioinformatic functionality to a SampleResultFolder."""

    @property
    def is_fastq(self):
        return self.module_name in FASTQ_MODULE_NAMES

    @property
    def is_short_read(self):
        return self.module_name.startswith('short_read')

    @property
    def is_paired_end(self):
        return self.module_name.endswith('paired_end')

    def read_file(self, read_id='read_1::lane_1'):
        assert self.is_fastq
        if 'read_2' in read_id:  # paired end
            assert self.is_paired_end

        module_parts = self.module_name.split('::')
        module_prefix = module_parts[0]
        module_suffix = module_parts[1] if len(module_parts) > 1 else None

        if read_id.startswith(f'{self.module_name}::'):
            file_name = read_id
        elif module_suffix and read_id.startswith(f'{module_suffix}::'):
            file_name = f'{module_prefix}::{read_id}'
        else:
            file_name = f'{self.module_name}::{read_id}'

        result_file = self.result_file(file_name)
        return result_file

    def read_1(self, lane_id='lane_1'):
        assert self.is_fastq
        file_name = f'{self.module_name}::read_1::{lane_id}'
        result_file = self.result_file(file_name)
        return result_file

    def read_2(self, lane_id='lane_1'):
        assert self.is_fastq
        assert self.is_paired_end
        file_name = f'{self.module_name}::read_2::{lane_id}'
        result_file = self.result_file(file_name)
        return result_file

    @classmethod
    def fastq_folder(
        cls,
        sample,
        read_length='short_read',
        paired_end=False,
        long_read_type='nanopore',
    ):
        if read_length == 'short_read':
            if paired_end:
                module_name = 'short_read::paired_end'
            else:
                module_name = 'short_read::single_end'
        elif read_length == 'long_read':
            module_name = 'long_read::{long_read_type}'
        else:
            raise ValueError(f'Invalid read_length: {read_length}')
        return sample.result_folder(module_name)
