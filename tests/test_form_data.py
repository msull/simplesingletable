import pytest

from simplesingletable import DynamoDbMemory
from simplesingletable.extras.form_data import (
    FormConfig,
    FormDataType,
    FormDataEntryField,
    Form,
    FormEntry,
    FormDataManager,
    FormDataRow,
    NewFormRequest,
    UpdateFormRequest,
    StoredFormData,
)


@pytest.fixture
def form_manager(dynamodb_memory: DynamoDbMemory):
    """Create a FormDataManager instance for testing."""
    return FormDataManager(memory=dynamodb_memory)


@pytest.fixture
def sample_schema():
    """Create a sample form schema."""
    return [
        FormDataEntryField(name="title", field_type="str", allowed_values=None),
        FormDataEntryField(name="value", field_type="int", allowed_values=None),
        FormDataEntryField(name="status", field_type="str", allowed_values=["active", "inactive"]),
        FormDataEntryField(name="enabled", field_type="bool", allowed_values=None),
    ]


@pytest.fixture
def form_data_type(form_manager: FormDataManager, sample_schema):
    """Create a sample FormDataType."""
    return form_manager.add_new_type(name="test_form_type", schema=sample_schema)


@pytest.fixture
def sample_form(form_manager: FormDataManager, form_data_type: FormDataType):
    """Create a sample Form."""
    form_request = NewFormRequest(
        name="Test Form",
        category="test_category",
        form_data_type_id=form_data_type.resource_id,
        form_data_type_version=form_data_type.version,
        form_data_type_schema=form_data_type.entry_schema,
        columns=["Column1", "Column2", "Column3"],
        groups=["Group1", "Group2"],
    )
    # Add the category first
    form_manager.add_form_category("test_category")
    return form_manager.create_form(form_request)


class TestFormConfig:
    """Test cases for FormConfig singleton."""

    def test_ensure_exists_creates_singleton(self, dynamodb_memory: DynamoDbMemory):
        """Test that ensure_exists creates a singleton instance."""
        config = FormConfig.ensure_exists(dynamodb_memory)
        assert config is not None
        assert isinstance(config.categories, set)
        assert len(config.categories) == 0

    def test_ensure_exists_returns_same_instance(self, dynamodb_memory: DynamoDbMemory):
        """Test that ensure_exists returns the same instance on subsequent calls."""
        config1 = FormConfig.ensure_exists(dynamodb_memory)
        config2 = FormConfig.ensure_exists(dynamodb_memory)
        assert config1.resource_id == config2.resource_id


class TestFormDataType:
    """Test cases for FormDataType management."""

    def test_add_new_type(self, form_manager: FormDataManager, sample_schema):
        """Test creating a new FormDataType."""
        form_type = form_manager.add_new_type(name="test_type", schema=sample_schema)
        assert form_type.name == "test_type"
        assert form_type.resource_id == "test_type"  # ID should be the name
        assert len(form_type.entry_schema) == 4
        assert form_type.entry_schema[0].name == "title"
        assert form_type.entry_schema[0].field_type == "str"

    def test_get_type(self, form_manager: FormDataManager, sample_schema):
        """Test retrieving an existing FormDataType."""
        created_type = form_manager.add_new_type(name="retrievable_type", schema=sample_schema)
        retrieved_type = form_manager.get_type("retrievable_type")
        assert retrieved_type.resource_id == created_type.resource_id
        assert retrieved_type.name == created_type.name
        assert retrieved_type.entry_schema == created_type.entry_schema

    def test_update_schema(self, form_manager: FormDataManager, sample_schema):
        """Test updating a FormDataType schema."""
        form_type = form_manager.add_new_type(name="updatable_type", schema=sample_schema)

        new_schema = sample_schema + [FormDataEntryField(name="description", field_type="str", allowed_values=None)]

        updated_type = form_manager.update_schema(form_type, new_schema)
        assert len(updated_type.entry_schema) == 5
        assert updated_type.entry_schema[-1].name == "description"
        assert updated_type.version == form_type.version + 1

    def test_list_available_types(self, form_manager: FormDataManager, sample_schema):
        """Test listing available FormDataTypes."""
        form_manager.add_new_type(name="type1", schema=sample_schema)
        form_manager.add_new_type(name="type2", schema=sample_schema)

        types = form_manager.list_available_types()
        assert len(types) >= 2
        type_names = [t.name for t in types]
        assert "type1" in type_names
        assert "type2" in type_names


class TestFormCategories:
    """Test cases for form category management."""

    def test_add_form_category(self, form_manager: FormDataManager):
        """Test adding a new form category."""
        form_manager.add_form_category("new_category")
        categories = form_manager.list_form_categories()
        assert "new_category" in categories

    def test_remove_form_category(self, form_manager: FormDataManager):
        """Test removing a form category."""
        form_manager.add_form_category("removable_category")
        categories = form_manager.list_form_categories()
        assert "removable_category" in categories

        form_manager.remove_form_category("removable_category")
        categories = form_manager.list_form_categories()
        assert "removable_category" not in categories

    def test_cannot_remove_category_with_forms(self, form_manager: FormDataManager, form_data_type: FormDataType):
        """Test that a category with forms cannot be removed."""
        form_manager.add_form_category("occupied_category")

        form_request = NewFormRequest(
            name="Blocking Form",
            category="occupied_category",
            form_data_type_id=form_data_type.resource_id,
            form_data_type_version=form_data_type.version,
            form_data_type_schema=form_data_type.entry_schema,
            columns=["Col1"],
            groups=["Group1"],
        )
        form_manager.create_form(form_request)

        with pytest.raises(ValueError, match="Cannot remove category that has forms assigned"):
            form_manager.remove_form_category("occupied_category")


class TestForm:
    """Test cases for Form CRUD operations."""

    def test_create_form(self, form_manager: FormDataManager, form_data_type: FormDataType):
        """Test creating a new Form."""
        form_manager.add_form_category("creation_test")
        form_request = NewFormRequest(
            name="Creation Test Form",
            category="creation_test",
            form_data_type_id=form_data_type.resource_id,
            form_data_type_version=form_data_type.version,
            form_data_type_schema=form_data_type.entry_schema,
            columns=["Col1", "Col2"],
            groups=["GroupA", "GroupB"],
        )

        form = form_manager.create_form(form_request)
        assert form.name == "Creation Test Form"
        assert form.category == "creation_test"
        assert form.columns == ["Col1", "Col2"]
        assert form.groups == ["GroupA", "GroupB"]

    def test_get_form(self, form_manager: FormDataManager, sample_form: Form):
        """Test retrieving an existing Form."""
        retrieved_form = form_manager.get_form(sample_form.resource_id)
        assert retrieved_form.resource_id == sample_form.resource_id
        assert retrieved_form.name == sample_form.name
        assert retrieved_form.columns == sample_form.columns

    def test_update_form(self, form_manager: FormDataManager, sample_form: Form):
        """Test updating a Form."""
        update_request = UpdateFormRequest(name="Updated Form Name", user_metadata={"key": "value"})

        updated_form = form_manager.update_form(sample_form, update_request)
        assert updated_form.name == "Updated Form Name"
        assert updated_form.user_metadata == {"key": "value"}
        assert updated_form.version == sample_form.version + 1

    def test_list_forms(self, form_manager: FormDataManager, sample_form: Form):
        """Test listing all forms."""
        forms = form_manager.list_forms()
        assert len(forms) >= 1
        form_ids = [f.resource_id for f in forms]
        assert sample_form.resource_id in form_ids

    def test_list_forms_by_category(self, form_manager: FormDataManager, sample_form: Form):
        """Test listing forms by category."""
        forms = form_manager.list_forms(category="test_category")
        assert len(forms) >= 1
        assert all(f.category == "test_category" for f in forms)

    def test_delete_column(self, form_manager: FormDataManager, sample_form: Form):
        """Test deleting a column from a form."""
        updated_form = form_manager.delete_column_from_form(sample_form, "Column2")
        assert "Column2" in updated_form.get_deleted_columns()
        assert "Column2" not in updated_form.get_ordered_columns()
        assert updated_form.columns == sample_form.columns  # Original columns unchanged
        assert 1 in updated_form.deleted_columns

    def test_restore_column(self, form_manager: FormDataManager, sample_form: Form):
        """Test restoring a deleted column."""
        updated_form = form_manager.delete_column_from_form(sample_form, "Column2")
        restored_form = form_manager.restore_column_to_form(updated_form, "Column2")
        assert "Column2" not in restored_form.get_deleted_columns()
        assert "Column2" in restored_form.get_ordered_columns()
        assert 1 not in restored_form.deleted_columns

    def test_column_display_order(self, form_manager: FormDataManager, sample_form: Form):
        """Test updating column display order."""
        new_order = ["Column3", "Column1", "Column2"]
        updated_form = form_manager.update_form_column_display_order(sample_form, new_order)
        assert updated_form.column_display_order == new_order
        assert updated_form.get_ordered_columns() == new_order

    def test_get_ordered_columns_with_hidden(self, sample_form: Form):
        """Test getting ordered columns with group-specific hidden columns."""
        sample_form.hide_columns_by_group = {"Group1": [1]}  # Hide Column2 for Group1

        # All columns for Group2
        cols_group2 = sample_form.get_ordered_columns("Group2")
        assert cols_group2 == ["Column1", "Column2", "Column3"]

        # Column2 hidden for Group1
        cols_group1 = sample_form.get_ordered_columns("Group1")
        assert cols_group1 == ["Column1", "Column3"]

    def test_summary_column(self, sample_form: Form):
        """Test that summary_column returns the first field name."""
        assert sample_form.summary_column == "title"


class TestFormEntry:
    """Test cases for FormEntry storage and retrieval."""

    def test_store_single_form_data(self, form_manager: FormDataManager, sample_form: Form):
        """Test storing a single FormEntry."""
        data = StoredFormData(
            col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "Test Title", "value": 42}
        )

        entry = form_manager.store_form_data(sample_form, data)
        assert isinstance(entry, FormEntry)
        assert entry.col_idx == 0
        assert entry.row_identifier == "row1"
        assert entry.group_identifier == "Group1"
        assert entry.data == {"title": "Test Title", "value": 42}

    def test_store_multiple_form_data(self, form_manager: FormDataManager, sample_form: Form):
        """Test storing multiple FormEntries."""
        data_list = [
            StoredFormData(
                col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "Title1", "value": 1}
            ),
            StoredFormData(
                col_idx=1, row_identifier="row1", group_identifier="Group1", data={"title": "Title2", "value": 2}
            ),
            StoredFormData(
                col_idx=2, row_identifier="row1", group_identifier="Group1", data={"title": "Title3", "value": 3}
            ),
        ]

        entries = form_manager.store_form_data(sample_form, data_list)
        assert len(entries) == 3
        assert all(isinstance(e, FormEntry) for e in entries)

    def test_update_existing_form_data(self, form_manager: FormDataManager, sample_form: Form):
        """Test updating an existing FormEntry."""
        # Create initial entry
        initial_data = StoredFormData(
            col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "Initial", "value": 1}
        )
        entry = form_manager.store_form_data(sample_form, initial_data)

        # Update the entry
        updated_data = StoredFormData(
            col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "Updated", "value": 2}
        )
        updated_entry = form_manager.store_form_data(sample_form, (entry, updated_data))

        assert updated_entry.data == {"title": "Updated", "value": 2}
        assert updated_entry.version == entry.version + 1

    def test_retrieve_all_form_entries(self, form_manager: FormDataManager, sample_form: Form):
        """Test retrieving all entries for a form."""
        # Store some entries
        data_list = [
            StoredFormData(col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "Entry1"}),
            StoredFormData(col_idx=1, row_identifier="row1", group_identifier="Group1", data={"title": "Entry2"}),
            StoredFormData(col_idx=0, row_identifier="row2", group_identifier="Group1", data={"title": "Entry3"}),
        ]
        form_manager.store_form_data(sample_form, data_list)

        # Retrieve all entries
        entries = FormEntry.retrieve_all_form_entries_for_form(form_manager.memory, sample_form)
        assert len(entries) == 3

    def test_retrieve_entries_by_group(self, form_manager: FormDataManager, sample_form: Form):
        """Test retrieving entries for a specific group."""
        # Store entries in different groups
        data_list = [
            StoredFormData(col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "G1Entry"}),
            StoredFormData(col_idx=0, row_identifier="row1", group_identifier="Group2", data={"title": "G2Entry"}),
        ]
        form_manager.store_form_data(sample_form, data_list)

        # Retrieve Group1 entries only
        group1_entries = FormEntry.retrieve_all_form_entries_for_form(form_manager.memory, sample_form, group="Group1")
        assert all(e.group_identifier == "Group1" for e in group1_entries)

    @pytest.mark.xfail(reason="Retrieving without group currently broken")
    def test_retrieve_entries_for_row(self, form_manager: FormDataManager, sample_form: Form):
        """Test retrieving all entries for a specific row."""
        # Store entries for different rows
        data_list = [
            StoredFormData(col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "R1C1"}),
            StoredFormData(col_idx=1, row_identifier="row1", group_identifier="Group1", data={"title": "R1C2"}),
            StoredFormData(col_idx=0, row_identifier="row2", group_identifier="Group1", data={"title": "R2C1"}),
        ]
        form_manager.store_form_data(sample_form, data_list)

        # Retrieve row1 entries
        row1_entries = FormEntry.retrieve_all_entries_for_row(form_manager.memory, sample_form, row_identifier="row1")
        assert all(e.row_identifier == "row1" for e in row1_entries)
        assert len(row1_entries) == 2


class TestFormDataMapping:
    """Test cases for FormDataMapping interface."""

    def test_create_mapping(self, form_manager: FormDataManager, sample_form: Form):
        """Test creating a FormDataMapping."""
        mapping = form_manager.get_mapping(sample_form)
        assert mapping.form.resource_id == sample_form.resource_id
        assert mapping.active_group in sample_form.groups

    def test_mapping_getitem(self, form_manager: FormDataManager, sample_form: Form):
        """Test accessing rows through mapping."""
        # Store some data
        data = StoredFormData(col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "Test"})
        form_manager.store_form_data(sample_form, data)

        mapping = form_manager.get_mapping(sample_form)
        mapping.switch_active_group("Group1")

        row = mapping["row1"]
        assert isinstance(row, FormDataRow)
        assert row.row_identifier == "row1"

    def test_mapping_iteration(self, form_manager: FormDataManager, sample_form: Form):
        """Test iterating over mapping."""
        # Store data for multiple rows
        data_list = [
            StoredFormData(col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "R1"}),
            StoredFormData(col_idx=0, row_identifier="row2", group_identifier="Group1", data={"title": "R2"}),
        ]
        form_manager.store_form_data(sample_form, data_list)

        mapping = form_manager.get_mapping(sample_form)
        mapping.switch_active_group("Group1")

        row_ids = list(mapping)
        assert "row1" in row_ids
        assert "row2" in row_ids

    def test_mapping_to_list(self, form_manager: FormDataManager, sample_form: Form):
        """Test converting mapping to list format."""
        # Store comprehensive data
        data_list = [
            StoredFormData(
                col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "Title1", "value": 1}
            ),
            StoredFormData(
                col_idx=1, row_identifier="row1", group_identifier="Group1", data={"title": "Title2", "value": 2}
            ),
        ]
        form_manager.store_form_data(sample_form, data_list)

        mapping = form_manager.get_mapping(sample_form)
        mapping.switch_active_group("Group1")

        # Test summary data (default)
        list_data = mapping.to_list()
        assert len(list_data) == 1
        assert list_data[0]["row_identifier"] == "row1"
        assert list_data[0]["group_identifier"] == "Group1"
        assert list_data[0]["Column1"] == "Title1"  # Summary field
        assert list_data[0]["Column2"] == "Title2"  # Summary field

        # Test full data
        full_data = mapping.to_list(summary_data=False)
        assert full_data[0]["Column1"]["value"] == 1

    def test_mapping_switch_group(self, form_manager: FormDataManager, sample_form: Form):
        """Test switching active group in mapping."""
        # Store data in different groups
        data_list = [
            StoredFormData(col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "G1"}),
            StoredFormData(col_idx=0, row_identifier="row2", group_identifier="Group2", data={"title": "G2"}),
        ]
        form_manager.store_form_data(sample_form, data_list)

        mapping = form_manager.get_mapping(sample_form)

        # Check Group1
        mapping.switch_active_group("Group1")
        assert "row1" in list(mapping)
        assert "row2" not in list(mapping)

        # Switch to Group2
        mapping.switch_active_group("Group2")
        assert "row2" in list(mapping)
        assert "row1" not in list(mapping)


class TestFormDataRow:
    """Test cases for FormDataRow interface."""

    def test_row_getitem_by_name(self, form_manager: FormDataManager, sample_form: Form):
        """Test accessing columns by name in a row."""
        # Store data
        data_list = [
            StoredFormData(col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "Col1Data"}),
            StoredFormData(col_idx=1, row_identifier="row1", group_identifier="Group1", data={"title": "Col2Data"}),
        ]
        form_manager.store_form_data(sample_form, data_list)

        mapping = form_manager.get_mapping(sample_form)
        mapping.switch_active_group("Group1")
        row = mapping["row1"]

        # Access by column name
        col1_entry = row["Column1"]
        assert col1_entry.data["title"] == "Col1Data"

        col2_entry = row["Column2"]
        assert col2_entry.data["title"] == "Col2Data"

    def test_row_getitem_by_index(self, form_manager: FormDataManager, sample_form: Form):
        """Test accessing columns by index in a row."""
        # Store data
        data = StoredFormData(col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "IndexTest"})
        form_manager.store_form_data(sample_form, data)

        mapping = form_manager.get_mapping(sample_form)
        mapping.switch_active_group("Group1")
        row = mapping["row1"]

        # Access by index
        first_col = row[0]
        assert first_col.data["title"] == "IndexTest"

    def test_row_iteration(self, form_manager: FormDataManager, sample_form: Form):
        """Test iterating over columns in a row."""
        mapping = form_manager.get_mapping(sample_form)
        mapping.switch_active_group("Group1")
        row = mapping["row1"]  # Even without data, should return empty row

        columns = list(row)
        assert columns == ["Column1", "Column2", "Column3"]

    def test_row_with_hidden_columns(self, form_manager: FormDataManager, sample_form: Form):
        """Test row behavior with hidden columns."""
        # Update form to hide Column2 for Group1
        sample_form.hide_columns_by_group = {"Group1": [1]}
        updated_form = form_manager.memory.update_existing(sample_form, {"hide_columns_by_group": {"Group1": [1]}})

        # Store data
        data_list = [
            StoredFormData(col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "Col1"}),
            StoredFormData(col_idx=1, row_identifier="row1", group_identifier="Group1", data={"title": "Col2Hidden"}),
            StoredFormData(col_idx=2, row_identifier="row1", group_identifier="Group1", data={"title": "Col3"}),
        ]
        form_manager.store_form_data(updated_form, data_list)

        mapping = form_manager.get_mapping(updated_form)
        mapping.switch_active_group("Group1")
        row = mapping["row1"]

        # Column2 should not be in iteration
        columns = list(row)
        assert "Column2" not in columns
        assert len(columns) == 2

        # But can still access hidden column explicitly with ignore_hidden_columns
        hidden_entry = row.get_item_by_key("Column2", ignore_hidden_columns=True)
        assert hidden_entry.data["title"] == "Col2Hidden"

    def test_row_invalid_access(self, form_manager: FormDataManager, sample_form: Form):
        """Test invalid column access in a row."""
        mapping = form_manager.get_mapping(sample_form)
        mapping.switch_active_group("Group1")
        row = mapping["row1"]

        # Invalid column name
        with pytest.raises(KeyError):
            _ = row["InvalidColumn"]

        # Invalid index
        with pytest.raises(KeyError):
            _ = row[99]


class TestFormDataIntegration:
    """Integration tests for the complete form data workflow."""

    def test_complete_workflow(self, form_manager: FormDataManager):
        """Test a complete workflow from type creation to data retrieval."""
        # Step 1: Create a form type
        schema = [
            FormDataEntryField(name="name", field_type="str", allowed_values=None),
            FormDataEntryField(name="score", field_type="int", allowed_values=None),
            FormDataEntryField(name="grade", field_type="str", allowed_values=["A", "B", "C", "D", "F"]),
        ]
        form_type = form_manager.add_new_type(name="student_grades", schema=schema)

        # Step 2: Create a category
        form_manager.add_form_category("academic")

        # Step 3: Create a form
        form_request = NewFormRequest(
            name="Math Class Grades",
            category="academic",
            form_data_type_id=form_type.resource_id,
            form_data_type_version=form_type.version,
            form_data_type_schema=form_type.entry_schema,
            columns=["Test1", "Test2", "Final"],
            groups=["Section A", "Section B"],
        )
        form = form_manager.create_form(form_request)

        # Step 4: Store student data
        student_data = [
            # Student 1, Section A
            StoredFormData(
                col_idx=0,
                row_identifier="student1",
                group_identifier="Section A",
                data={"name": "Alice", "score": 95, "grade": "A"},
            ),
            StoredFormData(
                col_idx=1,
                row_identifier="student1",
                group_identifier="Section A",
                data={"name": "Alice", "score": 88, "grade": "B"},
            ),
            StoredFormData(
                col_idx=2,
                row_identifier="student1",
                group_identifier="Section A",
                data={"name": "Alice", "score": 92, "grade": "A"},
            ),
            # Student 2, Section A
            StoredFormData(
                col_idx=0,
                row_identifier="student2",
                group_identifier="Section A",
                data={"name": "Bob", "score": 78, "grade": "C"},
            ),
            StoredFormData(
                col_idx=1,
                row_identifier="student2",
                group_identifier="Section A",
                data={"name": "Bob", "score": 82, "grade": "B"},
            ),
            StoredFormData(
                col_idx=2,
                row_identifier="student2",
                group_identifier="Section A",
                data={"name": "Bob", "score": 85, "grade": "B"},
            ),
        ]
        form_manager.store_form_data(form, student_data)

        # Step 5: Retrieve and verify data
        mapping = form_manager.get_mapping(form)
        mapping.switch_active_group("Section A")

        # Check we have two students
        assert len(mapping) == 2
        assert "student1" in mapping
        assert "student2" in mapping

        # Check Alice's grades
        alice = mapping["student1"]
        assert alice["Test1"].data["score"] == 95
        assert alice["Test2"].data["score"] == 88
        assert alice["Final"].data["score"] == 92

        # Convert to list format
        grades_list = mapping.to_list(summary_data=False)
        assert len(grades_list) == 2

        # Verify list format
        alice_data = next(g for g in grades_list if g["row_identifier"] == "student1")
        assert alice_data["Test1"]["score"] == 95
        assert alice_data["Test2"]["grade"] == "B"

    def test_versioning_with_updates(self, form_manager: FormDataManager, sample_form: Form):
        """Test that form entries maintain version history."""
        # Store initial data
        initial_data = StoredFormData(
            col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "Version1", "value": 1}
        )
        entry_v1 = form_manager.store_form_data(sample_form, initial_data)

        # Update the same entry
        update_data = StoredFormData(
            col_idx=0, row_identifier="row1", group_identifier="Group1", data={"title": "Version2", "value": 2}
        )
        entry_v2 = form_manager.store_form_data(sample_form, (entry_v1, update_data))

        # Verify versioning
        assert entry_v2.version == entry_v1.version + 1
        assert entry_v2.data["title"] == "Version2"

        # Retrieve historical version
        historical = form_manager.memory.read_existing(entry_v1.resource_id, FormEntry, version=entry_v1.version)
        assert historical.data["title"] == "Version1"
